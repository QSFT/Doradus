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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.utilities.Timer;

/**
 * Implements a {@link DBService} for Amazon's DynamoDB. Implementation notes and
 * limitations:
 * <ol>
 * <li>DynamoDB restricts row to 400KB size. Nothing is done to compensate for this limit
 *     yet. This means something will probably blow-up when a row gets too large.
 * <li>When running in an EC2 instance, DynamoDB will throttle responses, throwing
 *     exceptions when bandwidth is exceed. The code to handle this is not well tested.
 * <li>DynamoDB does not support namespaces. Each tenant must be placed in a DynamoDB
 *     instance with unique credentials and/or a unique region. Calls to
 *     {@link #createNamespace()} or {@link #dropNamespace()} will throw an exception.
 * <li>Tables are created with an attribute named "_key" as the row (item) key. Only
 *     hash-only keys are currently used. The _key attribute is removed from query results
 *     since the row key is handled independently.
 * <li>DynamoDB doesn't seem to allow null string values, despite its documentation. So,
 *     we store "null" columns by storing the value {@link #NULL_COLUMN_MARKER}.
 * </ol>
 * Because of multi-tenancy, AWS SDK standard Java properties and environment variables
 * are not used to define DynamoDB parameters. Instead, parameters must be defined for
 * each tenant as follows:<p>
 * <ul>
 * <li>Credentials: These are required. There are two ways to define parameters that
 *     identify the AWS credentials to use:
 *     <ol>
 *     <li><code>aws_profile</code>: This parameter defines a AWS profile name. By default,
 *         the profile lives in the file ~/.aws/credentials, but this location can be
 *         overridden by setting the parameter <code>aws_credential_file</code>. Using
 *         the <code>aws_profile</code> parameter is preferred since it is more secure.
 *     <li><code>aws_access_key</code> and <code>aws_secret_key</code>: These parameters
 *         must be defined if <code>aws_profile</code> is not defined.
 *     </ol>
 * <li>Endpoint: This is required and can be specified by using either of two parameters:
 *     <ol>
 *     <li><code>ddb.region</code>: When this parameter is set, it must define a valid AWS
 *         region name.
 *     <li><code>ddb.endpoint</code>: This is a connection string to a DynamoDB instance.
 *         This technique works when using a local DynamoDB instance for testing.
 *     </ol>
 * <li>Default capacity: These parameters are optional:
 *     <ol>
 *     <li><code>ddb_default_read_capacity</code>: This parameter defines the default
 *         read units for new tables.
 *     <li><code>ddb_default_write_capacity</code>: This parameter defines the default
 *         write units for new tables.
 * </ul>
 */
public class DynamoDBService extends DBService {
    // Special marker values:
    public static final String ROW_KEY_ATTR_NAME = "_key";
    public static final String NULL_COLUMN_MARKER = "\u0000";
    
    private static long READ_CAPACITY_UNITS = 1L;
    private static long WRITE_CAPACITY_UNITS = 1L;
    
    // Private members:
    private AmazonDynamoDBClient m_ddbClient;
    
    // Parameters:
    private final int m_retry_wait_millis;
    private final int m_max_commit_attempts;
    private final int m_max_read_attempts;
    private final String m_tenantPrefix;
    
    private DynamoDBService(Tenant tenant) {
        super(tenant);
        m_retry_wait_millis = getParamInt("retry_wait_millis", 5000);
        m_max_commit_attempts = getParamInt("max_commit_attempts", 10);
        m_max_read_attempts = getParamInt("max_read_attempts", 3);
        m_tenantPrefix = Utils.isEmpty(tenant.getNamespace()) ? "" : tenant.getNamespace() + "_"; 
        
        m_ddbClient = new AmazonDynamoDBClient(getCredentials());
        setRegionOrEndPoint();
        setDefaultCapacity();
    }
    
    //----- Service methods

    /**
     * Return the singleton DynamoDBService service object.
     * 
     * @return  Static DynamoDBService object.
     */
    public static DynamoDBService instance(Tenant tenant) {
        return new DynamoDBService(tenant);
    }
    
    /**
     * Establish a connection to DynamoDB.
     */
    @Override
    protected void initService() {
    }

    @Override
    protected void startService() {
        // nothing extra todo
    }

    @Override
    protected void stopService() {
        m_ddbClient.shutdown();
    }

    //----- Public DBService methods: Namespace management
    
    @Override
    public void createNamespace() {
        // Nothing to do.
    }
    
    @Override
    public void dropNamespace() {
        throw new RuntimeException("Namespaces are not supported");
    }
    
    //----- Public DBService methods: Store management
    
    @Override
    public void createStoreIfAbsent(String storeName, boolean bBinaryValues) {
        checkState();
        String tableName = storeToTableName(storeName);
        if (!Tables.doesTableExist(m_ddbClient, tableName)) {
            // Create a table with a primary hash key named '_key', which holds a string
            m_logger.info("Creating table: {}", tableName);
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
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
                Tables.awaitTableToBecomeActive(m_ddbClient, tableName);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);  
            }
        }
    }

    @Override
    public void deleteStoreIfPresent(String storeName) {
        checkState();
        String tableName = storeToTableName(storeName);
        m_logger.info("Deleting table: {}", tableName);
        try {
            m_ddbClient.deleteTable(new DeleteTableRequest(tableName));
            for (int seconds = 0; seconds < 10; seconds++) {
                try {
                    m_ddbClient.describeTable(tableName);
                    Thread.sleep(1000);
                } catch (ResourceNotFoundException e) {
                    break;  // Success
                }   // All other exceptions passed to outer try/catch
            }
        } catch (ResourceNotFoundException e) {
            // Already deleted.
        } catch (Exception e) {
            throw new RuntimeException("Error deleting table: " + tableName, e);
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
        checkState();
        new DDBTransaction(this).commit(dbTran);
    }
    
    //----- Public DBService methods: Queries

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey,
                                    String startColumn, String endColumn, int count) {
        checkState();
        String tableName = storeToTableName(storeName);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(tableName, makeDDBKey(rowKey)).getItem();
        List<DColumn> colList =
            loadAttributes(attributeMap,
                           colName -> ((Utils.isEmpty(startColumn) || colName.compareTo(startColumn) >= 0) &&
                                       (Utils.isEmpty(endColumn) || colName.compareTo(endColumn) <= 0))
                          );
        m_logger.debug("getColumns({}, {}, {}, {}) returning {} columns",
                       new Object[]{tableName, rowKey, startColumn, endColumn, colList.size()});
        return colList;
    }

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames) {
        checkState();
        String tableName = storeToTableName(storeName);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(tableName, makeDDBKey(rowKey)).getItem();
        List<DColumn> colList =
            loadAttributes(attributeMap,
                           colName -> (columnNames == null || columnNames.contains(colName))
                          );
        m_logger.debug("getColumns({}, {}, {} names) returning {} columns",
                       new Object[]{tableName, rowKey, columnNames.size(), colList.size()});
        return colList;
    }

    @Override
    public List<String> getRows(String storeName, String continuationToken, int count) {
        checkState();
        String tableName = storeToTableName(storeName);
        ScanRequest scanRequest = new ScanRequest(tableName);
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
    
    static String getDDBKey(Map<String, AttributeValue> key) {
        return key.get(ROW_KEY_ATTR_NAME).getS();
    }
    
    static Map<String, AttributeValue> makeDDBKey(String rowKey) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ROW_KEY_ATTR_NAME, new AttributeValue(rowKey));
        return key;
    }
    
    // Delete row and back off if ProvisionedThroughputExceededException occurs.
    void deleteRow(String storeName, Map<String, AttributeValue> key) {
        String tableName = storeToTableName(storeName);
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
                if (attempts >= m_max_commit_attempts) {
                    String errMsg = "All retries exceeded; abandoning deleteRow() for table: " + tableName;
                    m_logger.error(errMsg, e);
                    throw new RuntimeException(errMsg, e);
                }
                m_logger.warn("deleteRow() attempt #{} failed: {}", attempts, e);
                try {
                    Thread.sleep(attempts * m_retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }
    
    // Update item and back off if ProvisionedThroughputExceededException occurs.
    void updateRow(String storeName,
                   Map<String, AttributeValue> key,
                   Map<String, AttributeValueUpdate> attributeUpdates) {
        String tableName = storeToTableName(storeName);
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
                if (attempts >= m_max_commit_attempts) {
                    String errMsg = "All retries exceeded; abandoning updateRow() for table: " + tableName;
                    m_logger.error(errMsg, e);
                    throw new RuntimeException(errMsg, e);
                }
                m_logger.warn("updateRow() attempt #{} failed: {}", attempts, e);
                try {
                    Thread.sleep(attempts * m_retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }

    //----- Private methods

    // Prefix store name with tenant prefix if any.
    private String storeToTableName(String storeName) {
        return m_tenantPrefix + storeName;
    }

    // Set the AWS credentials in m_ddbClient
    private AWSCredentials getCredentials() {
        String awsProfile = getParamString("aws_profile");
        if (!Utils.isEmpty(awsProfile)) {
            m_logger.info("Using AWS profile: {}", awsProfile);
            ProfileCredentialsProvider credsProvider = null;
            String awsCredentialsFile = getParamString("aws_credentials_file");
            if (!Utils.isEmpty(awsCredentialsFile)) {
                credsProvider = new ProfileCredentialsProvider(awsCredentialsFile, awsProfile);
            } else {
                credsProvider = new ProfileCredentialsProvider(awsProfile);
            }
            return credsProvider.getCredentials();
        }
        
        String awsAccessKey = getParamString("aws_access_key");
        Utils.require(!Utils.isEmpty(awsAccessKey),
                      "Either 'aws_profile' or 'aws_access_key' must be defined for tenant: " + m_tenant.getName());
        String awsSecretKey = getParamString("aws_secret_key");
        Utils.require(!Utils.isEmpty(awsSecretKey),
                      "'aws_secret_key' must be defined when 'aws_access_key' is defined. " +
                      "'aws_profile' is preferred over aws_access_key/aws_secret_key. Tenant: " + m_tenant.getName());
        return new BasicAWSCredentials(awsAccessKey, awsSecretKey);
    }
    
    // Set the region or endpoint in m_ddbClient
    private void setRegionOrEndPoint() {
        String regionName = getParamString("ddb_region");
        if (regionName != null) {
            Regions regionEnum = Regions.fromName(regionName);
            Utils.require(regionEnum != null, "Unknown 'ddb_region': " + regionName);
            m_logger.info("Using region: {}", regionName);
            m_ddbClient.setRegion(Region.getRegion(regionEnum));
        } else {
            String ddbEndpoint = getParamString("ddb_endpoint");
            Utils.require(ddbEndpoint != null,
                          "Either 'ddb_region' or 'ddb_endpoint' must be defined for tenant: " + m_tenant.getName());
            m_logger.info("Using endpoint: {}", ddbEndpoint);
            m_ddbClient.setEndpoint(ddbEndpoint);
        }
    }

    // Set READ_CAPACITY_UNITS and WRITE_CAPACITY_UNITS if overridden.
    private void setDefaultCapacity() {
        Object capacity = getParam("ddb_default_read_capacity"); 
        if (capacity != null) {
            READ_CAPACITY_UNITS = Integer.parseInt(capacity.toString());
        }
        capacity = getParam("ddb_default_write_capacity");
        if (capacity != null) {
            WRITE_CAPACITY_UNITS = Integer.parseInt(capacity.toString());
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
                if (attempts >= m_max_read_attempts) {
                    String errMsg = "All retries exceeded; abandoning scan() for table: " + scanRequest.getTableName();
                    m_logger.error(errMsg, e);
                    throw new RuntimeException(errMsg, e);
                }
                m_logger.warn("scan() attempt #{} failed: {}", attempts, e);
                try {
                    Thread.sleep(attempts * m_retry_wait_millis);
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
