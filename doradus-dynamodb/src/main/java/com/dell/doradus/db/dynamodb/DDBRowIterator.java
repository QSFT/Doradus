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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.dell.doradus.service.db.DRow;

/**
 * Implements Iterator<DRow> for the DynamoDB service. All rows are captured and stored
 * in the constructor.
 */
public class DDBRowIterator implements Iterator<DRow> {
    private final List<DDBRow> m_rowList = new ArrayList<>();
    private int m_index;

    /**
     * Create a row iterator that returns all columns and rows for the given table. A scan
     * request is performed repeatedly until all rows are captured.
     * 
     * @param ddbClient Client for accessing DynamoDB API. 
     * @param tableName Name for which to get all rows.
     */
    public DDBRowIterator(AmazonDynamoDBClient ddbClient, String tableName) {
        // Use a scan request.
        ScanRequest scanRequest = new ScanRequest(tableName);
        while (true) {
            ScanResult scanResult = ddbClient.scan(scanRequest);
            List<Map<String, AttributeValue>> itemList = scanResult.getItems();
            if (itemList.size() == 0) {
                break;
            }
            for (Map<String, AttributeValue> attributeMap : itemList) {
                AttributeValue rowAttr = attributeMap.get(DynamoDBService.ROW_KEY_ATTR_NAME);
                m_rowList.add(new DDBRow(rowAttr.getS(), new DDBColumnIterator(attributeMap)));
            }
            Map<String, AttributeValue> lastEvaluatedKey = scanResult.getLastEvaluatedKey();
            if (lastEvaluatedKey == null) {
                break;
            }
            scanRequest.setExclusiveStartKey(lastEvaluatedKey);
        }
    }

    /**
     * Create a row iterator that returns all columns for specific rows for the given
     * table. A series of batch item requests are made if needed. 
     * 
     * @param ddbClient Client for accessing DynamoDB API. 
     * @param tableName Name for which to get rows for.
     * @param rowKeys   Collection of row keys to get.
     */
    public DDBRowIterator(AmazonDynamoDBClient ddbClient, String tableName, Collection<String> rowKeys) {
        this(ddbClient, tableName, rowKeys, null);
    }
    
    /**
     * Create a row iterator that returns specific rows and columns for the given table. A
     * series of batch requests are made if needed.
     * 
     * @param ddbClient Client for accessing DynamoDB API. 
     * @param tableName Name for which to get rows for.
     * @param rowKeys   Collection of row keys to get.
     * @param colNames  Set of column names to fetch. If null or empty, all columns are
     *                  fetched.
     */
    public DDBRowIterator(AmazonDynamoDBClient ddbClient, String tableName,
                          Collection<String> rowKeys, Collection<String> colNames) {
        Iterator<String> rowKeyIter = rowKeys.iterator();
        KeysAndAttributes keysAndAttributes = makeKeyBatch(rowKeyIter);
        
        while (true) {
            BatchGetItemRequest batchRequest = new BatchGetItemRequest();
            batchRequest.addRequestItemsEntry(tableName, keysAndAttributes);
            
            BatchGetItemResult batchResult = ddbClient.batchGetItem(batchRequest);
            Map<String, List<Map<String, AttributeValue>>> responseMap = batchResult.getResponses();
            
            List<Map<String, AttributeValue>> itemsList = responseMap.get(tableName);
            if (itemsList == null || itemsList.size() == 0) {
                break;
            }
            for (Map<String, AttributeValue> attributeMap : itemsList) {
                AttributeValue rowAttr = attributeMap.get(DynamoDBService.ROW_KEY_ATTR_NAME);
                m_rowList.add(new DDBRow(rowAttr.getS(), new DDBColumnIterator(attributeMap, colNames)));
            }
            
            Map<String, KeysAndAttributes> unprocessedKeys = batchResult.getUnprocessedKeys();
            if (unprocessedKeys != null && unprocessedKeys.containsKey(tableName)) {
                keysAndAttributes = unprocessedKeys.get(tableName);
            } else if (rowKeyIter.hasNext()) {
                keysAndAttributes = makeKeyBatch(rowKeyIter);
            } else {
                break;
            }
        }
    }

    /**
     * Create a row iterator that returns specific rows and column ranges for the given
     * table. A series of batch item requests are made if needed.
     * 
     * @param ddbClient Client for accessing DynamoDB API. 
     * @param tableName Name for which to get rows for.
     * @param rowKeys   Collection of row keys to get.
     * @param startCol  If non-null/empty, only column names greater than or equal to this
     *                  name are captured.
     * @param endCol    If non-null/empty, only column names less than or equal to this
     *                  name are captured.
     */
    public DDBRowIterator(AmazonDynamoDBClient ddbClient, String tableName,
                          Collection<String> rowKeys, String startCol, String endCol) {
        Iterator<String> rowKeyIter = rowKeys.iterator();
        KeysAndAttributes keysAndAttributes = makeKeyBatch(rowKeyIter);
        while (true) {
            BatchGetItemRequest batchRequest = new BatchGetItemRequest();
            batchRequest.addRequestItemsEntry(tableName, keysAndAttributes);
            
            BatchGetItemResult batchResult = ddbClient.batchGetItem(batchRequest);
            Map<String, List<Map<String, AttributeValue>>> responseMap = batchResult.getResponses();
            
            List<Map<String, AttributeValue>> itemsList = responseMap.get(tableName);
            if (itemsList == null || itemsList.size() == 0) {
                break;
            }
            for (Map<String, AttributeValue> attributeMap : itemsList) {
                AttributeValue rowAttr = attributeMap.get(DynamoDBService.ROW_KEY_ATTR_NAME);
                m_rowList.add(new DDBRow(rowAttr.getS(), new DDBColumnIterator(attributeMap, startCol, endCol)));
            }
            
            Map<String, KeysAndAttributes> unprocessedKeys = batchResult.getUnprocessedKeys();
            if (unprocessedKeys != null && unprocessedKeys.containsKey(tableName)) {
                keysAndAttributes = unprocessedKeys.get(tableName);
            } else if (rowKeyIter.hasNext()) {
                keysAndAttributes = makeKeyBatch(rowKeyIter);
            } else {
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return m_index < m_rowList.size();
    }

    @Override
    public DRow next() {
        return m_rowList.get(m_index++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        if (m_rowList.size() == 0) {
            buffer.append("Rows: <none>");
        } else {
            buffer.append("Rows:\n");
        }
        for (int index = 0; index < m_rowList.size(); index++) {
            if (index > 0) {
                buffer.append("\n");
            }
            buffer.append("   " + m_rowList.toString());
        }
        return buffer.toString();
    }
    
    // Map up to 100 keys to a DynamoDB key list.
    private KeysAndAttributes makeKeyBatch(Iterator<String> rowKeyIter) {
        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        for (int count = 0; count < 100 && rowKeyIter.hasNext(); count++) {
            keys.add(DynamoDBService.makeDDBKey(rowKeyIter.next()));
        }
        KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        keysAndAttributes.setKeys(keys);
        return keysAndAttributes;
    }
    
}   // class DDBRowIterator
