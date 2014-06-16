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

package com.dell.doradus.service.db.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.service.db.DBTransaction;

/**
 * Concrete DBTransaction for use with the Cassandra CQL API. Updates are stored in
 * separate maps for column adds/updates and row/column deletions. No timestamp is
 * maintained since CQL chooses the timestamp when the batch is applied.
 */
public class CQLTransaction extends DBTransaction {
    // Map of table name -> row key -> CQLColumn list.
    private final Map<String, Map<String, List<CQLColumn>>> m_updateMap = new HashMap<>();
    
    // Map of table name -> row key -> list of column names. If a row's column name list
    // is null or empty, the whole row is to be deleted.
    private final Map<String, Map<String, List<String>>> m_deleteMap = new HashMap<>();
    
    // Total updates performed.
    private int m_updates;

    /**
     * Start a new CQLTransaction.
     */
    public CQLTransaction() { }

    //----- General methods
    
    @Override
    public void clear() {
        m_updateMap.clear();
        m_deleteMap.clear();
        m_updates = 0;
    }

    @Override
    public int getUpdateCount() {
        return m_updates;
    }

    //----- Application schema update methods
    
    @Override
    public void addAppColumn(String appName, String colName, String colValue) {
        addColumn(CQLService.APPS_TABLE_NAME, appName, colName, colValue);
    }   // addAppColumn

    @Override
    public void deleteAppRow(String appName) {
        deleteRow(CQLService.APPS_TABLE_NAME, appName);
    }

    //----- Column/row update methods
    
    // Null column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName) {
        addColumn(storeName, rowKey, colName, "");
    }
    
    // String column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName, String colValue) {
        String tableName = CQLService.storeToCQLName(storeName);
        List<CQLColumn> colList = getUpdateColList(tableName, rowKey);
        colList.add(new CQLColumn(colName, colValue));
        m_updates++;
    }
    
    // Binary column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName, byte[] colValue) {
        String tableName = CQLService.storeToCQLName(storeName);
        List<CQLColumn> colList = getUpdateColList(tableName, rowKey);
        colList.add(new CQLColumn(colName, colValue));
        m_updates++;
    }

    // Long column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName, long colValue) {
        addColumn(storeName, rowKey, colName, Long.toString(colValue));
    }

    // Add row key to list of deletes.
    @Override
    public void deleteRow(String storeName, String rowKey) {
        String tableName = CQLService.storeToCQLName(storeName);
        Map<String, List<String>> rowKeyMap = getDeleteColMap(tableName);
        rowKeyMap.put(rowKey, null);
        m_updates++;
    }   // deleteRow

    // Add a deletion for a single column
    @Override
    public void deleteColumn(String storeName, String rowKey, String colName) {
        String tableName = CQLService.storeToCQLName(storeName);
        Map<String, List<String>> rowKeyMap = getDeleteColMap(tableName);
        List<String> colList = null;
        if (rowKeyMap.containsKey(rowKey)) {
            colList = rowKeyMap.get(rowKey);
            if (colList == null) {
                // Row is being deleted. Warn about this for now.
                m_logger.warn("deleteColumn() called for row being deleted; ignored. " +
                              "table={}, row={}, column={}",
                              new Object[]{tableName, rowKey, colName});
                return;
            }
        }
        if (colList == null) {
            colList = new ArrayList<>();
            rowKeyMap.put(rowKey, colList);
        }
        colList.add(colName);
        m_updates++;
    }   // deleteColumn

    // Add a deletion for a collection of columns
    @Override
    public void deleteColumns(String storeName, String rowKey, Collection<String> colNames) {
        String tableName = CQLService.storeToCQLName(storeName);
        Map<String, List<String>> rowKeyMap = getDeleteColMap(tableName);
        List<String> colList = null;
        if (rowKeyMap.containsKey(rowKey)) {
            colList = rowKeyMap.get(rowKey);
            if (colList == null) {
                // Row is being deleted. Warn about this for now.
                m_logger.warn("deleteColumns() called for row being deleted; ignored. " +
                              "table={}, row={}, # of columns={}",
                              new Object[]{tableName, rowKey, colNames.size()});
                return;
            }
        }
        if (colList == null) {
            colList = new ArrayList<>();
            rowKeyMap.put(rowKey, colList);
        }
        colList.addAll(colNames);
        m_updates += colNames.size();
    }   // deleteColumns

    //----- Local public methods
    
    /**
     * Get the update map, which holds all table/row/column updates.
     * 
     * @return  Map keyed by table name -> row key -> CQLColumn list.
     */
    public Map<String, Map<String, List<CQLColumn>>> getUpdateMap() {
        return m_updateMap;
    }   // getUpdateMap

    /**
     * Get the delete map, which holds all rows and columns to be deleted for all tables.
     * 
     * @return  Deletion map, which is keyed table name -> row key -> column name list.
     *          If the column name list is null for a given row, the entire row is to be
     *          deleted.
     */
    public Map<String, Map<String, List<String>>> getDeleteMap() {
        return m_deleteMap;
    }   // getDeleteMap
    
    //----- Private methods
    
    // Get the column list for the given table/row, adding the outer maps if needed.
    private List<CQLColumn> getUpdateColList(String tableName, String rowKey) {
        Map<String, List<CQLColumn>> rowMap = m_updateMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(tableName, rowMap);
        }
        List<CQLColumn> colList = rowMap.get(rowKey);
        if (colList == null) {
            colList = new ArrayList<>();
            rowMap.put(rowKey, colList);
        }
        return colList;
    }   // getUpdateColList

    // Get the delete row/column map for the given table, adding the outer map if needed.
    private Map<String, List<String>> getDeleteColMap(String tableName) {
        Map<String, List<String>> rowMap = m_deleteMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_deleteMap.put(tableName, rowMap);
        }
        return rowMap;
    }   // getDeleteColMap
    
}   // class CQLTransaction
