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

package com.dell.doradus.service.db.fs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;

public class FsTransaction extends DBTransaction {
    // Map of table name -> row key -> CQLColumn list.
    private final Map<String, Map<String, List<DColumn>>> m_updateMap = new HashMap<>();
    
    // Map of table name -> row key -> list of column names. If a row's column name list
    // is null or empty, the whole row is to be deleted.
    private final Map<String, Map<String, List<String>>> m_deleteMap = new HashMap<>();
    
    private final String m_keyspace;
    private int m_updates;

    public FsTransaction(String keyspace) { m_keyspace = keyspace; }

    @Override public void clear() {
        m_updateMap.clear();
        m_deleteMap.clear();
        m_updates = 0;
    }

    public String getKeyspace() { return m_keyspace; }
    
    @Override public int getUpdateCount() {
        return m_updates;
    }

    @Override public void addColumn(String storeName, String rowKey, String colName) {
        addColumn(storeName, rowKey, colName, "");
    }
    
    @Override public void addColumn(String storeName, String rowKey, String colName, String colValue) {
        List<DColumn> colList = getUpdateColList(storeName, rowKey);
        colList.add(new DColumn(colName, colValue));
        m_updates++;
    }
    
    @Override public void addColumn(String storeName, String rowKey, String colName, byte[] colValue) {
        List<DColumn> colList = getUpdateColList(storeName, rowKey);
        colList.add(new DColumn(colName, colValue));
        m_updates++;
    }

    @Override public void addColumn(String storeName, String rowKey, String colName, long colValue) {
        addColumn(storeName, rowKey, colName, Long.toString(colValue));
    }

    @Override public void deleteRow(String storeName, String rowKey) {
        Map<String, List<String>> rowKeyMap = getDeleteColMap(storeName);
        rowKeyMap.put(rowKey, null);
        m_updates++;
    }

    @Override public void deleteColumn(String storeName, String rowKey, String colName) {
        Map<String, List<String>> rowKeyMap = getDeleteColMap(storeName);
        List<String> colList = null;
        if (rowKeyMap.containsKey(rowKey)) {
            colList = rowKeyMap.get(rowKey);
            if (colList == null) {
                // Row is being deleted. Warn about this for now.
                m_logger.warn("deleteColumn() called for row being deleted; ignored. " +
                              "table={}, row={}, column={}",
                              new Object[]{storeName, rowKey, colName});
                return;
            }
        }
        if (colList == null) {
            colList = new ArrayList<>();
            rowKeyMap.put(rowKey, colList);
        }
        colList.add(colName);
        m_updates++;
    }

    @Override public void deleteColumns(String storeName, String rowKey, Collection<String> colNames) {
        Map<String, List<String>> rowKeyMap = getDeleteColMap(storeName);
        List<String> colList = null;
        if (rowKeyMap.containsKey(rowKey)) {
            colList = rowKeyMap.get(rowKey);
            if (colList == null) {
                // Row is being deleted. Warn about this for now.
                m_logger.warn("deleteColumns() called for row being deleted; ignored. " +
                              "table={}, row={}, # of columns={}",
                              new Object[]{storeName, rowKey, colNames.size()});
                return;
            }
        }
        if (colList == null) {
            colList = new ArrayList<>();
            rowKeyMap.put(rowKey, colList);
        }
        colList.addAll(colNames);
        m_updates += colNames.size();
    }
    
    
    public Map<String, Map<String, List<DColumn>>> getUpdateMap() { return m_updateMap; }
    public Map<String, Map<String, List<String>>> getDeleteMap() { return m_deleteMap; }


    public List<DColumn> getUpdateColList(String tableName, String rowKey) {
        Map<String, List<DColumn>> rowMap = m_updateMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(tableName, rowMap);
        }
        List<DColumn> colList = rowMap.get(rowKey);
        if (colList == null) {
            colList = new ArrayList<>(1000);
            rowMap.put(rowKey, colList);
        }
        return colList;
    }

    public Map<String, List<String>> getDeleteColMap(String tableName) {
        Map<String, List<String>> rowMap = m_deleteMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>(1000);
            m_deleteMap.put(tableName, rowMap);
        }
        return rowMap;
    }
    
}
