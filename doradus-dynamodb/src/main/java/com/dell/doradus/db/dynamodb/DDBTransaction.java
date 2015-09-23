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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;

/**
 * Applies column and row updates held in a {@link DBTransaction} to a DynamoDB instance.
 */
public class DDBTransaction {
    protected final static Logger m_logger = LoggerFactory.getLogger(DDBTransaction.class.getSimpleName());    
    private DDBTransaction() {}
    
    // Apply all updates in m_updateMap.
    public static void commit(DBTransaction dbTran) {
        try {
            applyUpdates(dbTran);
        } catch (Exception e) {
            // All retries, if needed, failed.
            if (e instanceof AmazonServiceException) {
                String rawMessage = ((AmazonServiceException)e).getRawResponseContent();
                m_logger.error("Commit failed: {}; rawResponseContent={}", e, rawMessage);
            } else {
                m_logger.error("Commit failed", e);
            }
            throw e;
        } finally {
            dbTran.clear();
        }
    }
    
    //----- Private methods
    
    private static void applyUpdates(DBTransaction dbTran) {
        Map<String, Map<String, List<DColumn>>> colUpdatesMap = dbTran.getColumnUpdatesMap();
        for (String tableName : colUpdatesMap.keySet()) {
            updateTableColumnUpdates(tableName, colUpdatesMap.get(tableName));
        }
        
        Map<String, Map<String, List<String>>> colDeletesMap = dbTran.getColumnDeletesMap();
        for (String tableName : colDeletesMap.keySet()) {
            updateTableColumnDeletes(tableName, colDeletesMap.get(tableName));
        }
        
        Map<String, List<String>> rowDeletesMap = dbTran.getRowDeletesMap();
        for (String tableName : rowDeletesMap.keySet()) {
            for (String rowKey : rowDeletesMap.get(tableName)) {
                Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
                DynamoDBService.instance().deleteRow(tableName, key);
            }
        }
    }

    private static void updateTableColumnUpdates(String tableName, Map<String, List<DColumn>> rowMap) {
        for (String rowKey : rowMap.keySet()) {
            Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
            List<DColumn> colList = rowMap.get(rowKey);
            updateRowColumnUpdates(tableName, key, colList);
        }
    }

    private static void updateRowColumnUpdates(String tableName,
                                               Map<String, AttributeValue> key,
                                               List<DColumn> colList) {
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        for (DColumn col : colList) {
            AttributeValue attrValue = mapColumnValue(col);
            attributeUpdates.put(col.getName(), new AttributeValueUpdate(attrValue, AttributeAction.PUT));
        }
        DynamoDBService.instance().updateRow(tableName, key, attributeUpdates);
    }

    private static void updateTableColumnDeletes(String tableName, Map<String, List<String>> rowMap) {
        for (String rowKey : rowMap.keySet()) {
            Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
            List<String> colNames = rowMap.get(rowKey);
            updateRowColumnDeletes(tableName, key, colNames);
        }
    }

    private static void updateRowColumnDeletes(String tableName,
                                               Map<String, AttributeValue> key,
                                               List<String> colNames) {
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        for (String colName : colNames) {
            attributeUpdates.put(colName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
        }
        DynamoDBService.instance().updateRow(tableName, key, attributeUpdates);
    }

    // Create the appropriate AttributeValue for the given column value type and length.
    private static AttributeValue mapColumnValue(DColumn col) {
        AttributeValue attrValue = new AttributeValue();
        boolean isBinaryValue = DynamoDBService.instance().columnIsBinary(col.getName());
        if (isBinaryValue) {
            if (col.getRawValue().length == 0) {
                attrValue.setS(DynamoDBService.NULL_COLUMN_MARKER);
            } else {
                attrValue.setB(ByteBuffer.wrap(col.getRawValue()));
            }
        } else {
            String strValue = col.getValue();
            if (strValue.length() == 0) {
                strValue = DynamoDBService.NULL_COLUMN_MARKER;
            }
            attrValue.setS(strValue);
        }
        return attrValue;
    }
    
}   // class DDBTransaction
