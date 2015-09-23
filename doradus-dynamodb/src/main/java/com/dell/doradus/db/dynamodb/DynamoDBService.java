/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.db.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.dell.doradus.common.UserDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.utilities.Timer;

/**
 * Implements a {@link DBService} for Amazon's DynamoDB. This implementation is currently
 * experimental. Implementation notes and limitations:
 * <ol>
 * <li>DynamoDB restricts row to 400KB size. Nothing is done to compensate for this limit
 *     yet. This means something will probably blow-up when a row gets too large.
 * <li>When running in an EC2 instance, DynamoDB will throttle responses, throwing
 *     exceptions when bandwidth is exceed. The code to handle this is not well tested.
 * <li>This service only supports the default tenant. Commands such as
 *     {@link #createTenant()} (other than for the default tenant) and {@link #addUsers()}
 *     will throw an exception. 
 * <li>Tables are created with an attribute named "_key" as the row (item) key. Only
 *     hash-only keys are currently used. The _key attribute is removed from query results
 *     since the row key is handled independently.
 * <li>DynamoDB doesn't seem to allow null string values, despite its documentation. So,
 *     we store "null" columns by storing the value {@link #NULL_COLUMN_MARKER}.
 * </ol>
 * The connection to a DynamoDB is established on AWS SDK standard Java properties and
 * environment variables where possible. However, we define two additional Java properties
 * to define the database endpoint. Below is a summary of what must be set:
 * <ul>
 * <li>Credentials: There are two ways to define the credentials to use:
 *     <ol>
 *     <li>An access key and secret key can be explicitly provided by setting either the
 *         Java properties <code>aws.accessKeyId</code> and <code>aws.secretKey</code> or
 *         the environment variables <code>AWS_ACCESS_KEY_ID</code> and <code>
 *         AWS_SECRET_ACCESS_KEY</code>.
 *     <li>A profile name can be provided by setting the environment variable <code>
 *         AWS_PROFILE</code>. By default, the profile lives in the file ~/.aws/credentials
 *         but this can be overridden by setting the environment variable <code>
 *         AWS_CREDENTIAL_FILE</code>.
 *     </ol>
 *     The AWS SDK discoveries the credentials settings automatically.
 * <li>Endpoint: There are two ways to define the database endpoint:
 *     <ol>
 *     <li>Set the Java property <code>ddb.region</code>, which implies the endpoint.
 *     <li>Set the Java property <code>ddb.endpoint</code>. This technique works when
 *         using a local DynamoDB instance for testing.
 *     </ol>
 * </ul>
 *
 */
public class DynamoDBService extends DBService {
    // Java properties we define:
    public static final String DDB_REGION = "ddb.region";
    public static final String DDB_ENDPOINT = "ddb.endpoint";
    private static final String DDB_DEFAULT_READ_CAPACITY = "ddb.default.read.capacity";
    private static final String DDB_DEFAULT_WRITE_CAPACITY = "ddb.default.write.capacity";
    
    // Special marker values:
    public static final String ROW_KEY_ATTR_NAME = "_key";
    public static final String NULL_COLUMN_MARKER = "\u0000";
    
    private static long READ_CAPACITY_UNITS = 1L;
    private static long WRITE_CAPACITY_UNITS = 1L;
    
    // Singleton instance:
    private static DynamoDBService INSTANCE = new DynamoDBService();

    // Private members:
    private AmazonDynamoDBClient m_ddbClient;
    
    private DynamoDBService() { }
    
    //----- Service methods

    /**
     * Return the singleton DynamoDBService service object.
     * 
     * @return  Static DynamoDBService object.
     */
    public static DynamoDBService instance() {return INSTANCE;}
    
    /**
     * Establish a connection to DynamoDB.
     */
    @Override
    protected void initService() {
        m_ddbClient = new AmazonDynamoDBClient();
        setRegionOrEndPoint();
        setDefaultCapacity();
    }

    @Override
    protected void startService() {
        // nothing extra todo
    }

    @Override
    protected void stopService() {
        m_ddbClient.shutdown();
    }

    //----- Public DBService methods: Tenant management
    
    @Override
    public void createTenant(Tenant tenant, Map<String, String> options) {
        checkTenant(tenant.getKeyspace());
    }

    @Override
    public void modifyTenant(Tenant tenant, Map<String, String> options) {
        checkTenant(tenant.getKeyspace());
        throw new UnsupportedOperationException("modifyTenant");
    }

    @Override
    public void dropTenant(Tenant tenant) {
        checkTenant(tenant.getKeyspace());
        throw new UnsupportedOperationException("dropTenant");
    }

    @Override
    public void addUsers(Tenant tenant, Iterable<UserDefinition> users) {
        checkTenant(tenant.getKeyspace());
        throw new UnsupportedOperationException("addUsers");
    }

    @Override
    public void modifyUsers(Tenant tenant, Iterable<UserDefinition> users) {
        checkTenant(tenant.getKeyspace());
        throw new UnsupportedOperationException("modifyUsers");
    }

    @Override
    public void deleteUsers(Tenant tenant, Iterable<UserDefinition> users) {
        checkTenant(tenant.getKeyspace());
        throw new UnsupportedOperationException("deleteUsers");
    }

    @Override
    public Collection<Tenant> getTenants() {
        checkState();
        List<Tenant> tenants = new ArrayList<Tenant>();
        tenants.add(new Tenant(ServerConfig.getInstance().keyspace));
        return tenants;
    }

    //----- Public DBService methods: Store management
    
    @Override
    public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
        checkTenant(tenant.getKeyspace());
        if (!Tables.doesTableExist(m_ddbClient, storeName)) {
            // Create a table with a primary hash key named '_key', which holds a string
            m_logger.info("Creating table: {}", storeName);
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(storeName)
                .withKeySchema(new KeySchemaElement()
                    .withAttributeName(ROW_KEY_ATTR_NAME)
                    .withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition()
                    .withAttributeName(ROW_KEY_ATTR_NAME)
                    .withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput()
                    .withReadCapacityUnits(READ_CAPACITY_UNITS)
                    .withWriteCapacityUnits(WRITE_CAPACITY_UNITS));
            m_ddbClient.createTable(createTableRequest).getTableDescription();
            try {
                Tables.awaitTableToBecomeActive(m_ddbClient, storeName);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);  
            }
        }
    }

    @Override
    public void deleteStoreIfPresent(Tenant tenant, String storeName) {
        checkTenant(tenant.getKeyspace());
        m_logger.info("Deleting table: {}", storeName);
        try {
            m_ddbClient.deleteTable(new DeleteTableRequest(storeName));
            for (int seconds = 0; seconds < 10; seconds++) {
                try {
                    m_ddbClient.describeTable(storeName);
                    Thread.sleep(1000);
                } catch (ResourceNotFoundException e) {
                    break;  // Success
                }   // All other exceptions passed to outer try/catch
            }
        } catch (ResourceNotFoundException e) {
            // Already deleted.
        } catch (Exception e) {
            throw new RuntimeException("Error deleting table: " + storeName, e);
        }
    }

    //----- Public DBService methods: Updates
    
    /**
     * Commit the updates in the given {@link DBTransaction}. An exception is thrown if
     * the updates cannot be committed after all retries. Regardless of whether the
     * updates are successful or not, the updates are cleared from the transaction object
     * before returning.
     * 
     * @param dbTran    {@link DBTransaction} containing updates to commit.
     */
    public void commit(DBTransaction dbTran) {
        checkTenant(dbTran.getNamespace());
        DDBTransaction.commit(dbTran);
    }
    
    //----- Public DBService methods: Queries

    @Override
    public List<DColumn> getColumns(String namespace, String storeName, String rowKey,
                                    String startColumn, String endColumn, int count) {
        checkTenant(namespace);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(storeName, makeDDBKey(rowKey)).getItem();
        List<DColumn> colList =
            loadAttributes(attributeMap,
                           colName -> ((Utils.isEmpty(startColumn) || colName.compareTo(startColumn) >= 0) &&
                                       (Utils.isEmpty(endColumn) || colName.compareTo(endColumn) <= 0))
                          );
        m_logger.debug("getColumns({}, {}, {}, {}) returning {} columns",
                       new Object[]{storeName, rowKey, startColumn, endColumn, colList.size()});
        return colList;
    }

    @Override
    public List<DColumn> getColumns(String namespace, String storeName, String rowKey, Collection<String> columnNames) {
        checkTenant(namespace);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(storeName, makeDDBKey(rowKey)).getItem();
        List<DColumn> colList =
            loadAttributes(attributeMap,
                           colName -> (columnNames == null || columnNames.contains(colName))
                          );
        m_logger.debug("getColumns({}, {}, {} names) returning {} columns",
                       new Object[]{storeName, rowKey, columnNames.size(), colList.size()});
        return colList;
    }

    @Override
    public List<String> getRows(String namespace, String storeName, String continuationToken, int count) {
        checkTenant(namespace);
        ScanRequest scanRequest = new ScanRequest(storeName);
        scanRequest.setAttributesToGet(Arrays.asList(ROW_KEY_ATTR_NAME)); // attributes to get
        if (continuationToken != null) {
            scanRequest.setExclusiveStartKey(makeDDBKey(continuationToken));
        }
        List<String> rowKeys = new ArrayList<>();
        while (rowKeys.size() < count) {
            ScanResult scanResult = scan(scanRequest);
            List<Map<String, AttributeValue>> itemList = scanResult.getItems();
            if (itemList.size() == 0) {
                break;
            }
            for (Map<String, AttributeValue> attributeMap : itemList) {
                AttributeValue rowAttr = attributeMap.get(ROW_KEY_ATTR_NAME);
                rowKeys.add(rowAttr.getS());
                if (rowKeys.size() >= count) {
                    break;
                }
            }
            Map<String, AttributeValue> lastEvaluatedKey = scanResult.getLastEvaluatedKey();
            if (lastEvaluatedKey == null) {
                break;
            }
            scanRequest.setExclusiveStartKey(lastEvaluatedKey);
        }
        return rowKeys;
    }

    //----- Package methods
    
    // TODO: This might not be necessary; see if every column should be considered binary.
    public boolean columnIsBinary(String name) {
        return true;
    }

    static String getDDBKey(Map<String, AttributeValue> key) {
        return key.get(ROW_KEY_ATTR_NAME).getS();
    }
    
    static Map<String, AttributeValue> makeDDBKey(String rowKey) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ROW_KEY_ATTR_NAME, new AttributeValue(rowKey));
        return key;
    }
    
    // Delete row and back off if ProvisionedThroughputExceededException occurs.
    void deleteRow(String tableName, Map<String, AttributeValue> key) {
        m_logger.debug("Deleting row from table {}, key={}", tableName, DynamoDBService.getDDBKey(key));
        
        Timer timer = new Timer();
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                m_ddbClient.deleteItem(tableName, key);
                if (attempts > 1) {
                    m_logger.info("deleteRow() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
                m_logger.debug("Time to delete table {}, key={}: {}",
                               new Object[]{tableName, DynamoDBService.getDDBKey(key), timer.toString()});
            } catch (ProvisionedThroughputExceededException e) {
                if (attempts >= ServerConfig.getInstance().max_commit_attempts) {
                    String errMsg = "All retries exceeded; abandoning deleteRow() for table: " + tableName;
                    m_logger.error(errMsg, e);
                    throw new RuntimeException(errMsg, e);
                }
                m_logger.warn("deleteRow() attempt #{} failed: {}", attempts, e);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }
    
    // Update item and back off if ProvisionedThroughputExceededException occurs.
    void updateRow(String tableName,
                   Map<String, AttributeValue> key,
                   Map<String, AttributeValueUpdate> attributeUpdates) {
        m_logger.debug("Updating row in table {}, key={}", tableName, DynamoDBService.getDDBKey(key));
        
        Timer timer = new Timer();
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                m_ddbClient.updateItem(tableName, key, attributeUpdates);
                if (attempts > 1) {
                    m_logger.info("updateRow() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
                m_logger.debug("Time to update table {}, key={}: {}",
                               new Object[]{tableName, DynamoDBService.getDDBKey(key), timer.toString()});
            } catch (ProvisionedThroughputExceededException e) {
                if (attempts >= ServerConfig.getInstance().max_commit_attempts) {
                    String errMsg = "All retries exceeded; abandoning updateRow() for table: " + tableName;
                    m_logger.error(errMsg, e);
                    throw new RuntimeException(errMsg, e);
                }
                m_logger.warn("updateRow() attempt #{} failed: {}", attempts, e);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }

    //----- Private methods
    
    // Throw if not running or tenant is not default.
    private void checkTenant(String namespace) {
        checkState();
        Utils.require(namespace.equals(ServerConfig.getInstance().keyspace),
                      "Only the default is currently supported");
    }
    
    // Set the region or endpoint in m_ddbClient
    private void setRegionOrEndPoint() {
        String regionName = System.getProperty(DDB_REGION);
        if (regionName != null) {
            Regions regionEnum = Regions.fromName(regionName);
            if (regionEnum == null) {
                throw new RuntimeException("Unknown '" + DDB_REGION + "': " + regionName);
            }
            m_logger.info("Using region: {}", regionName);
            m_ddbClient.setRegion(Region.getRegion(regionEnum));
        } else {
            String ddbEndpoint = System.getProperty(DDB_ENDPOINT);
            if (ddbEndpoint == null) {
                throw new RuntimeException("Either '" + DDB_REGION + "' or '" + DDB_ENDPOINT + "' must be set");
            }
            m_logger.info("Using endpoint: {}", ddbEndpoint);
            m_ddbClient.setEndpoint(ddbEndpoint);
        }
    }

    // Set READ_CAPACITY_UNITS and WRITE_CAPACITY_UNITS if overridden.
    private void setDefaultCapacity() {
        String capacity = System.getProperty(DDB_DEFAULT_READ_CAPACITY);
        if (capacity != null) {
            READ_CAPACITY_UNITS = Integer.parseInt(capacity);
        }
        capacity = System.getProperty(DDB_DEFAULT_WRITE_CAPACITY);
        if (capacity != null) {
            WRITE_CAPACITY_UNITS = Integer.parseInt(capacity);
        }
        m_logger.info("Default table capacity: read={}, write={}", READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS);
    }

    // Perform a scan request and retry if ProvisionedThroughputExceededException occurs.
    private ScanResult scan(ScanRequest scanRequest) {
        m_logger.debug("Performing scan() request on table {}", scanRequest.getTableName());
        
        Timer timer = new Timer();
        boolean bSuccess = false;
        ScanResult scanResult = null;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                scanResult = m_ddbClient.scan(scanRequest);
                if (attempts > 1) {
                    m_logger.info("scan() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
                m_logger.debug("Time to scan table {}: {}", scanRequest.getTableName(), timer.toString());
            } catch (ProvisionedThroughputExceededException e) {
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
                    String errMsg = "All retries exceeded; abandoning scan() for table: " + scanRequest.getTableName();
                    m_logger.error(errMsg, e);
                    throw new RuntimeException(errMsg, e);
                }
                m_logger.warn("scan() attempt #{} failed: {}", attempts, e);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
        return scanResult;
    }
    
    // Filter, store, and sort attributes from the given map.
    private List<DColumn> loadAttributes(Map<String, AttributeValue> attributeMap,
                                         Predicate<String> colNamePredicate) {
        List<DColumn> columns = new ArrayList<>();
        if (attributeMap != null) {
            for (Map.Entry<String, AttributeValue> mapEntry : attributeMap.entrySet()) {
                String colName = mapEntry.getKey();
                if (!colName.equals(DynamoDBService.ROW_KEY_ATTR_NAME) && // Don't add row key attribute as a column
                                colNamePredicate.test(colName)) {
                    AttributeValue attrValue = mapEntry.getValue();
                    if (attrValue.getB() != null) {
                        columns.add(new DColumn(colName, Utils.getBytes(attrValue.getB())));
                    } else if (attrValue.getS() != null) {
                        String value = attrValue.getS();
                        if (value.equals(DynamoDBService.NULL_COLUMN_MARKER)) {
                            value = "";
                        }
                        columns.add(new DColumn(colName, value));
                    } else {
                        throw new RuntimeException("Unknown AttributeValue type: " + attrValue);
                    }
                }
            }
        }
        
        // Sort or reverse sort column names.
        Collections.sort(columns, new Comparator<DColumn>() {
            @Override public int compare(DColumn col1, DColumn col2) {
                return col1.getName().compareTo(col2.getName());
            }
        });
        return columns;
    }
    
}   // class DynamoDBService
