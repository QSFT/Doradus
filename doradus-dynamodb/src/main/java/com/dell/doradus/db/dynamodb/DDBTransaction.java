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
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

/**
 * Applies column and row updates held in a {@link DBTransaction} to a DynamoDB instance.
 */
public class DDBTransaction {
    private final static Logger m_logger = LoggerFactory.getLogger(DDBTransaction.class.getSimpleName());
    private final DynamoDBService m_service;
    
    DDBTransaction(DynamoDBService service) { m_service = service; }
    
    /**
     * Apply aall updates in the given {@link DBTransaction} to our DynamoDB instance.
     * 
     * @param dbTran {@link DBTransaction} with updates.
     */
    public void commit(DBTransaction dbTran) {
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
    
    private void applyUpdates(DBTransaction dbTran) {
        Tenant tenant = dbTran.getTenant();
        Map<String, Map<String, List<DColumn>>> colUpdatesMap = dbTran.getColumnUpdatesMap();
        for (String tableName : colUpdatesMap.keySet()) {
            updateTableColumnUpdates(tenant, tableName, colUpdatesMap.get(tableName));
        }
        
        Map<String, Map<String, List<String>>> colDeletesMap = dbTran.getColumnDeletesMap();
        for (String tableName : colDeletesMap.keySet()) {
            updateTableColumnDeletes(tenant, tableName, colDeletesMap.get(tableName));
        }
        
        Map<String, List<String>> rowDeletesMap = dbTran.getRowDeletesMap();
        for (String tableName : rowDeletesMap.keySet()) {
            for (String rowKey : rowDeletesMap.get(tableName)) {
                Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
                m_service.deleteRow(tableName, key);
            }
        }
    }

    private void updateTableColumnUpdates(Tenant tenant, String tableName, Map<String, List<DColumn>> rowMap) {
        for (String rowKey : rowMap.keySet()) {
            Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
            List<DColumn> colList = rowMap.get(rowKey);
            updateRowColumnUpdates(tenant, tableName, key, colList);
        }
    }

    private void updateRowColumnUpdates(Tenant tenant,
                                        String tableName,
                                        Map<String, AttributeValue> key,
                                        List<DColumn> colList) {
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        for (DColumn col : colList) {
            AttributeValue attrValue = mapColumnValue(tenant, col);
            attributeUpdates.put(col.getName(), new AttributeValueUpdate(attrValue, AttributeAction.PUT));
        }
        m_service.updateRow(tableName, key, attributeUpdates);
    }

    private void updateTableColumnDeletes(Tenant tenant, String tableName, Map<String, List<String>> rowMap) {
        for (String rowKey : rowMap.keySet()) {
            Map<String, AttributeValue> key = DynamoDBService.makeDDBKey(rowKey);
            List<String> colNames = rowMap.get(rowKey);
            updateRowColumnDeletes(tenant, tableName, key, colNames);
        }
    }

    private void updateRowColumnDeletes(Tenant tenant,
                                        String tableName,
                                        Map<String, AttributeValue> key,
                                        List<String> colNames) {
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        for (String colName : colNames) {
            attributeUpdates.put(colName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
        }
        m_service.updateRow(tableName, key, attributeUpdates);
    }

    // Create the appropriate AttributeValue for the given column value type and length.
    private AttributeValue mapColumnValue(Tenant tenant, DColumn col) {
        AttributeValue attrValue = new AttributeValue();
        boolean isBinaryValue = ((DynamoDBService)DBService.instance(tenant)).columnIsBinary(col.getName());
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
