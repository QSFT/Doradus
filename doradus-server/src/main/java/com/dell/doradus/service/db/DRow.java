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

package com.dell.doradus.service.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Represents a row, which is a record stored in a Doradus store. A row has a key and a
 * collection of columns. The Row object will contain only those columns, if any, that 
 * were retrieved from the database.
 */
public class DRow {
    private String m_namespace;
    private String m_storeName;
    private String m_rowKey;
    
    public DRow(String namespace, String storeName, String rowKey) {
        m_namespace = namespace;
        m_storeName = storeName;
        m_rowKey = rowKey;
    }
    
    public String getNamespace() { return m_namespace; }
    public String getStoreName() { return m_storeName; }
    public String getRowKey() { return m_rowKey; }
    public String getKey() { return m_rowKey; } // for backward compatibility

    public Iterable<DColumn> getAllColumns(int chunkSize) {
        return getColumns(null, null, chunkSize);
    }
    
    public Iterable<DColumn> getColumns(String startColumn, String endColumn, int chunkSize) {
        return new SequenceIterable<DColumn>(new ColumnSequence(this, startColumn, endColumn, chunkSize));
    }
    
    public List<DColumn> getColumns(Collection<String> columnNames) {
        return DBService.instance().getColumns(m_namespace, m_storeName, m_rowKey, columnNames);
    }

    public DColumn getColumn(String columnName) {
        List<String> colNames = new ArrayList<String>(1);
        colNames.add(columnName);
        List<DColumn> columns = getColumns(colNames);
        if(columns.size() == 0) return null;
        else return columns.get(0);
    }
    
    public List<DColumn> getColumns(List<String> columnNames, int chunkSize) {
        if(chunkSize < columnNames.size()) {
            return getColumns(columnNames);
        }
        
        List<DColumn> columns = new ArrayList<>(columnNames.size());
        for(int start = 0; start < columnNames.size(); start += chunkSize) {
            int end = Math.min(start + chunkSize, columnNames.size());
            List<String> partialNames = columnNames.subList(start, end);
            List<DColumn> partialList = DBService.instance().getColumns(m_namespace, m_storeName, m_rowKey, partialNames);
            columns.addAll(partialList);
        }
        return columns;
    }

}
