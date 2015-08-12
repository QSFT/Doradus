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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.dell.doradus.service.db.DBTransaction;

/**
 * Holds a set of column and row updates that can be committed to DynamoDB.
 */
public class DDBTransaction extends DBTransaction {
    // Map of table name -> row key -> column name -> column value. Updates are represented as follows:
    //      add/update column: DDBColumn name and value are both set
    //          if value is empty, this is "null" to DynamoDB
    //      delete column: DDBColumn value is null
    //      delete row: List<DDBColumn> is null
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
        if (colMap.put(colName, null) == null) {
            m_updateCount++;
        }
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
            if (colMap.put(colName, null) == null) {
                m_updateCount++;
            }
        }
    }
    
    @Override
    public void deleteRow(String storeName, String rowKey) {
        Map<String, Map<String, Object>> rowMap = m_updateMap.get(storeName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(storeName, rowMap);
        }
        if (rowMap.put(rowKey, null) == null) {
            m_updateCount++;
        }
    }

    //----- DDBTransaction-specific methods
    
    // Apply all updates in m_updateMap.
    public void commit() {
        try {
            for (String tableName : m_updateMap.keySet()) {
                Map<String, Map<String, Object>> rowMap = m_updateMap.get(tableName);
                updateTable(tableName, rowMap);
            }
        } catch (Throwable e) {
            // All retries, if needed, failed.
            if (e instanceof AmazonServiceException) {
                String rawMessage = ((AmazonServiceException)e).getRawResponseContent();
                m_logger.error("Commit failed: {}; rawResponseContent={}", e, rawMessage);
                m_logger.debug("Failed transaction: {}", traceString());
            } else {
                m_logger.error("Commit failed", e);
            }
            throw e;
        }
    }

    @Override
    public String toString() {
        return m_updateCount + " updates for " + m_updateMap.size() + " tables";
    }
    
    //----- Private methods
    
    // Apply all updates for the given table.
    private void updateTable(String tableName, Map<String, Map<String, Object>> rowMap) {
        for (String rowKey : rowMap.keySet()) {
            Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
            Map<String, Object> colMap = rowMap.get(rowKey);
            if (colMap == null) {
                DynamoDBService.instance().deleteRow(tableName, key);
            } else {
                updateColumns(tableName, key, colMap);
            }
        }
    }
    
    // Add or delete columns for the given table and row.
    private void updateColumns(String tableName, Map<String, AttributeValue> key, Map<String, Object> colMap) {
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        for (String colName : colMap.keySet()) {
            Object colValue = colMap.get(colName);
            if (colValue == null) {
                attributeUpdates.put(colName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
            } else {
                AttributeValue attrValue = mapColumnValue(colValue);
                attributeUpdates.put(colName, new AttributeValueUpdate(attrValue, AttributeAction.PUT));
            }
        }
        DynamoDBService.instance().updateRow(tableName, key, attributeUpdates);
    }
    
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
        if (colMap.put(colName, colValue) == null) {
            m_updateCount++;
        }
    }
    
    // Create the appropriate AttributeVakue for the given column value type and length.
    private static AttributeValue mapColumnValue(Object colValue) {
        AttributeValue attrValue = new AttributeValue();
        if (colValue instanceof byte[]) {
            byte[] byteVal = (byte[])colValue;
            if (byteVal.length == 0) {
                attrValue.setS(DynamoDBService.NULL_COLUMN_MARKER);
            } else {
                attrValue.setB(ByteBuffer.wrap((byte[])colValue));
            }
        } else {
            String strValue = (String)colValue;
            if (strValue.length() == 0) {
                strValue = DynamoDBService.NULL_COLUMN_MARKER;
            }
            attrValue.setS(strValue);
        }
        return attrValue;
    }
    
    // Return a diagnostic string summarizing this transaction's updates.
    private String traceString() {
        StringBuilder buffer = new StringBuilder();
        for (String tableName : m_updateMap.keySet()) {
            buffer.append("Table " + tableName + ":\n");
            Map<String, Map<String, Object>> rowMap = m_updateMap.get(tableName);
            for (String rowKey : rowMap.keySet()) {
                Map<String, Object> colMap = rowMap.get(rowKey);
                if (colMap == null) {
                    buffer.append("   row " + "[" + rowKey + "]: deleted\n");
                } else {
                    int totalAdds = 0;
                    int totalDeletes = 0;
                    for (String colName : colMap.keySet()) {
                        if (colMap.get(colName) == null) {
                            totalDeletes++;
                        } else {
                            totalAdds++;
                        }
                    }
                    buffer.append("   row " + "[" + rowKey + "] adds: " + totalAdds + ", deletes: " + totalDeletes + "\n");
                }
            }
        }
        return buffer.toString();
    }
    
}   // class DDBTransaction
