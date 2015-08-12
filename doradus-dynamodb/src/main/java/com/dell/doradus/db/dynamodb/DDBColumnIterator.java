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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DColumn;

import java.util.Collections;

/**
 * Implement Iterator<DColumn> for the DynamoDB database service. All columns are fetched
 * in the constructor and stored in a sorted/filtered list.
 */
public class DDBColumnIterator implements Iterator<DColumn> {
    private final List<DColumn> m_columns = new ArrayList<>();
    private int m_index;
   
    // For filtering column names during capture.
    private interface ColumnFilter {
        boolean select(String colName);
    }
    
    /**
     * Create an iterator for all columns in the given attribute map.
     * 
     * @param attributeMap  Result of a DynamoDB scan or batch item request.
     */
    public DDBColumnIterator(Map<String, AttributeValue> attributeMap) {
        loadAttributes(attributeMap, false, new ColumnFilter() {
            @Override public boolean select(String colName) {return true;}
        });
    }
    
    /**
     * Create an iterator for columns in the given attribute map whose name is between
     * the given start and/or end column name. Column names are returned in ascending
     * order.
     * 
     * @param attributeMap  Result of a DynamoDB scan or batch item request.
     * @param startColumn   If non-null/empty, only column names greater than or equal to
     *                      this name are retained.
     * @param endColumn     If non-null/empty, only column names less than or equal to
     *                      this name are retained.
     */
    public DDBColumnIterator(Map<String, AttributeValue> attributeMap,
                             final String                startColumn,
                             final String                endColumn) {
        loadAttributes(attributeMap, false, new ColumnFilter() {
            @Override public boolean select(String colName) {
                if (!Utils.isEmpty(startColumn) && colName.compareTo(startColumn) < 0) {
                    return false;
                }
                if (!Utils.isEmpty(endColumn) && colName.compareTo(endColumn) > 0) {
                    return false;
                }
                return true;
            }
        });
    }
    
    /**
     * Create an iterator for columns in the given attribute map whose name is between
     * the given start and/or end column name. If bRervesed=false, column names are
     * returned in ascending order. If bReversed is true, column names are returned in
     * descending order, in which case startColumn must be >= endColumn. 
     * 
     * @param attributeMap  Result of a DynamoDB scan or batch item request.
     * @param startColumn   If non-null/empty, only column names greater than or equal
     *                      (bReversed=false) or less than or equal (bReversed=true) to
     *                      this are retained.
     * @param endColumn     If non-null/empty, only column names less than or equal
     *                      (bReversed=false) or less than or equal (bReversed=true) to
     *                      this name are retained.
     * @param bReversed     True to provide column names in reverse order and reverse the
     *                      filtering applied via startColumn and endColumn.
     */
    public DDBColumnIterator(Map<String, AttributeValue> attributeMap,
                             String                      startColumn,
                             String                      endColumn,
                             boolean                     bReversed) {
        final String start = bReversed ? endColumn : startColumn;
        final String end = bReversed ?startColumn : endColumn;
        loadAttributes(attributeMap, bReversed, new ColumnFilter() {
            @Override public boolean select(String colName) {
                if ((!Utils.isEmpty(start) && colName.compareTo(start) < 0) ||
                    (!Utils.isEmpty(end) && colName.compareTo(end) > 0)) {
                    return false;
                }
                return true;
            }
        });
    }

    /**
     * Create an iterator for columns in this given attribute map whose is contained in
     * the given column name collection.
     * 
     * @param attributeMap  Result of a DynamoDB scan or batch item request.
     * @param colNames      Collection (preferably a Set) of names in to include in the
     *                      iterator.
     */
    public DDBColumnIterator(Map<String, AttributeValue> attributeMap, final Collection<String> colNames) {
        loadAttributes(attributeMap, false, new ColumnFilter() {
            @Override public boolean select(String colName) {
                return colNames == null || colNames.contains(colName);
            }
        });
    }

    @Override
    public boolean hasNext() {
        return m_index < m_columns.size();
    }

    @Override
    public DColumn next() {
        return m_columns.get(m_index++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return m_columns.size() + " columns";
    }
    
    public String toVerboseString() {
        StringBuilder buffer = new StringBuilder();
        boolean bFirst = true;
        for (int index = 0; index < m_columns.size(); index++) {
            if (bFirst) {
                bFirst = false;
            } else {
                buffer.append(",");
            }
            buffer.append(m_columns.get(index).getName());
            buffer.append("=");
            buffer.append(m_columns.get(index).getValue());
        }
        if (bFirst) {
            buffer.append("<none>");
        }
        return buffer.toString();
    }
    
    //----- Private methods
    
    // Filter, store, and sort attributes from the given map.
    private void loadAttributes(Map<String, AttributeValue> attributeMap, final boolean bReversed, ColumnFilter filter) {
        if (attributeMap == null) {
            return;
        }
        for (Map.Entry<String, AttributeValue> mapEntry : attributeMap.entrySet()) {
            String colName = mapEntry.getKey();
            if (!colName.equals(DynamoDBService.ROW_KEY_ATTR_NAME) && // Don't add row key attribute as a column
                filter.select(colName)) {
                AttributeValue attrValue = mapEntry.getValue();
                if (attrValue.getB() != null) {
                    m_columns.add(new DColumn(colName, Utils.getBytes(attrValue.getB())));
                } else if (attrValue.getS() != null) {
                    String value = attrValue.getS();
                    if (value.equals(DynamoDBService.NULL_COLUMN_MARKER)) {
                        value = "";
                    }
                    m_columns.add(new DColumn(colName, value));
                } else {
                    throw new RuntimeException("Unknown AttributeValue type: " + attrValue);
                }
            }
        }
        
        // Sort or reverse sort column names.
        Collections.sort(m_columns, new Comparator<DColumn>() {
            @Override public int compare(DColumn col1, DColumn col2) {
                if (bReversed) {
                    return col2.getName().compareTo(col1.getName());
                } else {
                    return col1.getName().compareTo(col2.getName());
                }
            }
        });
    }
    
}   // class DDBColumnIterator
