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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.tenant.UserDefinition;

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
 *
 */
public class DynamoDBService extends DBService {
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
        String profileName = System.getProperty("DDB_PROFILE_NAME");
        if (profileName == null) {
            throw new RuntimeException("Property 'DDB_PROFILE_NAME' must be set to the desired DynamoDB profile name");
        }
        
        try {
            ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile();
            ProfileCredentialsProvider proCredProvider = new ProfileCredentialsProvider(profilesConfigFile, profileName);
            AWSCredentials credentials = proCredProvider.getCredentials();
            m_ddbClient = new AmazonDynamoDBClient(credentials);
        } catch (Exception e) {
            throw new RuntimeException("Cannot validate DynamoDB credentials", e);
        }
        
        String regionName = System.getProperty("DDB_REGION");
        if (regionName != null) {
            Regions regionEnum = Regions.fromName(regionName);
            if (regionEnum == null) {
                throw new RuntimeException("Unknown 'DDB_REGION': " + regionName);
            }
            m_ddbClient.setRegion(Region.getRegion(regionEnum));
        } else {
            String ddbEndpoint = System.getProperty("DDB_ENDPOINT");
            if (ddbEndpoint == null) {
                throw new RuntimeException("Either 'DDB_REGION' or 'DDB_ENDPOINT' must be set");
            }
            m_ddbClient.setEndpoint(ddbEndpoint);
        }
        
        String capacity = System.getProperty("DDB_DEFAULT_READ_CAPACITY");
        if (capacity != null) {
            READ_CAPACITY_UNITS = Integer.parseInt(capacity);
        }
        capacity = System.getProperty("DDB_DEFAULT_WRITE_CAPACITY");
        if (capacity != null) {
            WRITE_CAPACITY_UNITS = Integer.parseInt(capacity);
        }
        m_logger.info("Default table capacity: read={}, write={}", READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS);
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
        checkTenant(tenant);
    }

    @Override
    public void modifyTenant(Tenant tenant, Map<String, String> options) {
        checkTenant(tenant);
        throw new UnsupportedOperationException("modifyTenant");
    }

    @Override
    public void dropTenant(Tenant tenant) {
        checkTenant(tenant);
        throw new UnsupportedOperationException("dropTenant");
    }

    @Override
    public void addUsers(Tenant tenant, Iterable<UserDefinition> users) {
        checkTenant(tenant);
        throw new UnsupportedOperationException("addUsers");
    }

    @Override
    public void modifyUsers(Tenant tenant, Iterable<UserDefinition> users) {
        checkTenant(tenant);
        throw new UnsupportedOperationException("modifyUsers");
    }

    @Override
    public void deleteUsers(Tenant tenant, Iterable<UserDefinition> users) {
        checkTenant(tenant);
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
        checkTenant(tenant);
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
        checkTenant(tenant);
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
    
    @Override
    public DBTransaction startTransaction(Tenant tenant) {
        checkTenant(tenant);
        return new DDBTransaction();
    }

    @Override
    public void commit(DBTransaction dbTran) {
        checkState();
        m_logger.debug("Committing transaction: {}", dbTran.toString());
        ((DDBTransaction)dbTran).commit(m_ddbClient);
    }

    //----- Public DBService methods: Queries

    @Override
    public Iterator<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
        checkTenant(tenant);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(storeName, makeDDBKey(rowKey)).getItem();
        DDBColumnIterator ddbCol = new DDBColumnIterator(attributeMap);
        m_logger.debug("getAllColumns({}, {}) returning: {}",
                      new Object[]{storeName, rowKey, ddbCol.toString()});
        return ddbCol;
    }

    @Override
    public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName,
                                            String rowKey, String startCol,
                                            String endCol, boolean reversed) {
        checkTenant(tenant);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(storeName, makeDDBKey(rowKey)).getItem();
        DDBColumnIterator ddbCol = new DDBColumnIterator(attributeMap, startCol, endCol, reversed);
        m_logger.debug("getColumnSlice({}, {}, {}, {}) returning: {}",
                      new Object[]{storeName, rowKey, startCol, endCol, ddbCol.toString()});
        return ddbCol;
    }

    @Override
    public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName,
                                            String rowKey, String startCol, String endCol) {
        return getColumnSlice(tenant, storeName, rowKey, startCol, endCol, false);
    }

    @Override
    public Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName) {
        checkTenant(tenant);
        DDBRowIterator ddbRow = new DDBRowIterator(m_ddbClient, storeName);
        m_logger.debug("getAllRowsAllColumns({}) returning {}", storeName, ddbRow.toString());
        return ddbRow;
    }

    @Override
    public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
        checkTenant(tenant);
        Map<String, AttributeValue> attributeMap = m_ddbClient.getItem(storeName, makeDDBKey(rowKey)).getItem();
        if (attributeMap == null || !attributeMap.containsKey(colName)) {
            m_logger.debug("getColumn({}, {}, {}) returning <null>",
                          new Object[]{storeName, rowKey, colName});
            return null;
        }
        AttributeValue attrValue = attributeMap.get(colName);
        DColumn col = null;
        if (attrValue.getB() != null) {
            col = new DColumn(colName, Utils.getBytes(attrValue.getB()));
        } else if (attrValue.getS() != null) {
            String value = attrValue.getS();
            if (value.equals(NULL_COLUMN_MARKER)) {
                value = "";
            }
            col = new DColumn(colName, value);
        } else {
            throw new RuntimeException("Unknown AttributeValue type: " + attrValue);
        }
        m_logger.debug("getColumn({}, {}, {}) returning {}",
                      new Object[]{storeName, rowKey, col.toString()});
        return col;
    }

    @Override
    public Iterator<DRow> getRowsAllColumns(Tenant tenant, String storeName, Collection<String> rowKeys) {
        checkTenant(tenant);
        DDBRowIterator ddbRow = new DDBRowIterator(m_ddbClient, storeName, rowKeys);
        m_logger.debug("getRowsAllColumns({}, {}) returning {}",
                      new Object[]{storeName, rowKeys, ddbRow.toString()});
        return ddbRow;
    }

    @Override
    public Iterator<DRow> getRowsColumns(Tenant tenant, String storeName, Collection<String> rowKeys, Collection<String> colNames) {
        checkTenant(tenant);
        DDBRowIterator ddbRow = new DDBRowIterator(m_ddbClient, storeName, rowKeys, colNames);
        m_logger.debug("getRowsColumns({}, {}, {}) returning {}",
                      new Object[]{storeName, rowKeys, colNames, ddbRow.toString()});
        return ddbRow;
    }

    @Override
    public Iterator<DRow> getRowsColumnSlice(Tenant tenant, String storeName, Collection<String> rowKeys, String startCol, String endCol) {
        checkTenant(tenant);
        DDBRowIterator ddbRow = new DDBRowIterator(m_ddbClient, storeName, rowKeys, startCol, endCol);
        m_logger.debug("getRowsColumnSlice({}, {}, {}, {}) returning {}",
                      new Object[]{storeName, rowKeys, startCol, endCol, ddbRow.toString()});
        return ddbRow;
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
    
    //----- Private methods
    
    // Throw if not running or tenant is not default.
    private void checkTenant(Tenant tenant) {
        checkState();
        Utils.require(tenant.getKeyspace().equals(ServerConfig.getInstance().keyspace),
                        "Only the default is currently supported");
    }
    
}   // class DynamoDBService
