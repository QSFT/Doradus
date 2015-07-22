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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBTransaction;

/**
 * Holds a set of column and row updates that can be committed to DynamoDB.
 */
public class DDBTransaction extends DBTransaction {
    // Protected logger available to concrete services:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    // Map of table name -> row key -> column name -> column value. Updates are represented as follows:
    //      add/update column: DDBColumn name and value are both set
    //          if value is empty, this is "null" to DynamoDB
    //      delete column: DDBColumn value is null
    //      delete row: List<DDBColumn> is null or empty
    private final Map<String, Map<String, Map<String, Object>>> m_updateMap = new HashMap<>();
    private int m_updateCount;
    
    public DDBTransaction() {}
    
    //----- DBTransaction concrete methods
    
    @Override
    public void clear() {
        m_updateMap.clear();
        m_updateCount = 0;
    }

    @Override
    public int getUpdateCount() {
        return m_updateCount;
    }

    @Override
    public void addColumn(String storeName, String rowKey, String colName) {
        addColumnValue(storeName, rowKey, colName, "");
    }

    @Override
    public void addColumn(String storeName, String rowKey, String colName, String colValue) {
        addColumnValue(storeName, rowKey, colName, colValue);
    }

    @Override
    public void addColumn(String storeName, String rowKey, String colName, byte[] colValue) {
        addColumnValue(storeName, rowKey, colName, colValue);
    }

    @Override
    public void addColumn(String storeName, String rowKey, String colName, long colValue) {
        addColumnValue(storeName, rowKey, colName, Long.toString(colValue));
    }

    @Override
    public void deleteColumn(String storeName, String rowKey, String colName) {
        Map<String, Map<String, Object>> rowMap = m_updateMap.get(storeName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(storeName, rowMap);
        }
        Map<String, Object> colMap = rowMap.get(rowKey);
        if (colMap == null) {
            colMap = new HashMap<>();
            rowMap.put(rowKey, colMap);
        }
        colMap.put(colName, null);
        m_updateCount++;
    }
    
    @Override
    public void deleteColumns(String storeName, String rowKey, Collection<String> columns) {
        Map<String, Map<String, Object>> rowMap = m_updateMap.get(storeName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(storeName, rowMap);
        }
        Map<String, Object> colMap = rowMap.get(rowKey);
        if (colMap == null) {
            colMap = new HashMap<>();
            rowMap.put(rowKey, colMap);
        }
        for (String colName : columns) {
            colMap.put(colName, null);
            m_updateCount++;
        }
    }
    
    @Override
    public void deleteRow(String storeName, String rowKey) {
        Map<String, Map<String, Object>> rowMap = m_updateMap.get(storeName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(storeName, rowMap);
        }
        rowMap.put(rowKey, null);
        m_updateCount++;
    }

    //----- DDBTransaction-specific methods
    
    public void commit(AmazonDynamoDBClient ddbClient) {
        for (String tableName : m_updateMap.keySet()) {
            Map<String, Map<String, Object>> rowMap = m_updateMap.get(tableName);
            for (String rowKey : rowMap.keySet()) {
                Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
                
                Map<String, Object> colMap = rowMap.get(rowKey);
                if (colMap == null) {
                    deleteRow(ddbClient, tableName, key);
                } else {
                    Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
                    for (String colName : colMap.keySet()) {
                        Object colValue = colMap.get(colName);
                        if (colValue == null) {
                            attributeUpdates.put(colName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                        } else {
                            AttributeValue attrValue = new AttributeValue();
                            if (colValue instanceof byte[]) {
                                attrValue.setB(ByteBuffer.wrap((byte[])colValue));
                            } else {
                                String strValue = (String)colValue;
                                if (strValue.length() == 0) {
                                    strValue = DynamoDBService.NULL_COLUMN_MARKER;
                                }
                                attrValue.setS(strValue);
                            }
                            attributeUpdates.put(colName, new AttributeValueUpdate(attrValue, AttributeAction.PUT));
                        }
                    }
                    updateRow(ddbClient, tableName, key, attributeUpdates);
                }
            }
        }
    }

    @Override
    public String toString() {
        return m_updateCount + " updates for " + m_updateMap.size() + " tables";
    }
    
    public String traceString() {
        StringBuilder buffer = new StringBuilder();
        for (String tableName : m_updateMap.keySet()) {
            buffer.append("Table " + tableName + ":\n");
            Map<String, Map<String, Object>> rowMap = m_updateMap.get(tableName);
            for (String rowKey : rowMap.keySet()) {
                Map<String, Object> colMap = rowMap.get(rowKey);
                if (colMap == null) {
                    buffer.append("   delete row " + "[" + rowKey + "]\n");
                } else {
                    List<String> colDeletes = new ArrayList<>();
                    Map<String, String> colAdds = new HashMap<>();
                    for (String colName : colMap.keySet()) {
                        Object colValue = colMap.get(colName);
                        if (colValue == null) {
                            colDeletes.add(colName);
                        } else {
                            if (colValue instanceof byte[]) {
                                colAdds.put(colName, Utils.toString((byte[])colValue));
                            } else {
                                String strValue = (String)colValue;
                                if (strValue.length() == 0) {
                                    colAdds.put(colName, "<null>");
                                } else {
                                    colAdds.put(colName, strValue);
                                }
                            }
                        }
                    }
                    if (colAdds.size() > 0) {
                        buffer.append("   update row " + "[" + rowKey + "] add: " + colAdds.toString() + "\n");
                    }
                    if (colDeletes.size() > 0) {
                        buffer.append(tableName + "   update row " + "[" + rowKey + "] delete: " + colDeletes.toString() + "\n");
                    }
                }
            }
        }
        return buffer.toString();
    }
    
    //----- Private methods
    
    // Store the given table/row/column update. The column value may be empty.
    private void addColumnValue(String tableName, String rowKey, String colName, Object colValue) {
        Map<String, Map<String, Object>> rowMap = m_updateMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(tableName, rowMap);
        }
        
        Map<String, Object> colMap = rowMap.get(rowKey);
        if (colMap == null) {
            colMap = new HashMap<>();
            rowMap.put(rowKey, colMap);
        }
        colMap.put(colName, colValue);
        m_updateCount++;
    }
    
    // Update item and back off if ProvisionedThroughputExceededException occurs.
    private void updateRow(AmazonDynamoDBClient ddbClient,
                           String tableName,
                           Map<String, AttributeValue> key,
                           Map<String, AttributeValueUpdate> attributeUpdates) {
        m_logger.debug("Updating row in table {}, key={}", tableName, DynamoDBService.getDDBKey(key));
        
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                ddbClient.updateItem(tableName, key, attributeUpdates);
                if (attempts > 1) {
                    m_logger.info("updateRow() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (ProvisionedThroughputExceededException e) {
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
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

    // Delete row and back off if ProvisionedThroughputExceededException occurs.
    private void deleteRow(AmazonDynamoDBClient ddbClient,
                           String tableName,
                           Map<String, AttributeValue> key) {
        m_logger.debug("Deleting row from table {}, key={}", tableName, DynamoDBService.getDDBKey(key));
        
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                ddbClient.deleteItem(tableName, key);
                if (attempts > 1) {
                    m_logger.info("deleteRow() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (ProvisionedThroughputExceededException e) {
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
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
    
}   // class DDBTransaction
