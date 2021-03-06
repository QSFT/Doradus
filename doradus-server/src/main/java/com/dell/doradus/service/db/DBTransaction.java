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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.dell.doradus.common.Utils;

/**
 * Class that encapsulates a set of updates that will be committed together.
 * Provides methods to post updates based on a tabular model: add/replace column, delete
 * column, and delete row. These methods operate on a "store" (ColumnFamily in Cassandra).
 * <p>
 * The accumulated updates are commited by calling {@link DBService#commit(DBTransaction)}
 * which also clears the updates. Updates can also be cleared by calling {@link #clear()}.
 * The transaction object subsequently can be reused.
 */
public class DBTransaction {
    private static final byte[] EMPTY = new byte[0];
    // The Tenant of the transaction
    private Tenant m_tenant;
    //Column updates
    private Set<ColumnUpdate> m_columnUpdates = new HashSet<>();
    //Column deletes
    private Set<ColumnDelete> m_columnDeletes = new HashSet<>();
    //Row deletes
    private Set<RowDelete> m_rowDeletes = new HashSet<>();
    
    /**
     * Create a new DBTransaction.
     */
    public DBTransaction(Tenant tenant) {
        m_tenant = tenant;
    }
   
    /**
     * Clear all updates defined in this transaction, allowing to reuse it. The namespace does not change
     */
    public void clear() {
        m_columnUpdates.clear();
        m_columnDeletes.clear();
        m_rowDeletes.clear();
    }

    /**
     * Get the {@link Tenant} of the transaction.
     */
    public Tenant getTenant() {
        return m_tenant;
    }
    
    //----- Statistics
    
    /**
     * Get the number of mutations (column updates + column deletes + row deletes) in the transaction so far
     */
    public int getMutationsCount() {
        return m_columnUpdates.size() + m_columnDeletes.size() + m_rowDeletes.size();
    }
    
    //----- Column/row retrieval methods
    
    /**
     * Get the collection of the column updates in this transaction so far
     */
    public Collection<ColumnUpdate> getColumnUpdates() {
        return m_columnUpdates;
    }
    
    /**
     * Get the collection of the column deletes in this transaction so far
     */
    public Collection<ColumnDelete> getColumnDeletes() {
        return m_columnDeletes;
    }

    /**
     * Get the collection of the row deletes in this transaction so far
     */
    public Collection<RowDelete> getRowDeletes() {
        return m_rowDeletes;
    }

    /**
     * Get the map of the column updates as storeName -> rowKey -> list of DColumns
     */
    public Map<String, Map<String, List<DColumn>>> getColumnUpdatesMap() {
        Map<String, Map<String, List<DColumn>>> storeMap = new HashMap<>();
        for(ColumnUpdate mutation: getColumnUpdates()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            DColumn column = mutation.getColumn();
            Map<String, List<DColumn>> rowMap = storeMap.get(storeName);
            if(rowMap == null) {
                rowMap = new HashMap<>();
                storeMap.put(storeName, rowMap);
            }
            List<DColumn> columnsList = rowMap.get(rowKey);
            if(columnsList == null) {
                columnsList = new ArrayList<>();
                rowMap.put(rowKey, columnsList);
            }
            columnsList.add(column);
        }
        return storeMap;
    }

    /**
     * Get the map of the column deletes as storeName -> rowKey -> list of column names
     */
    public Map<String, Map<String, List<String>>> getColumnDeletesMap() {
        Map<String, Map<String, List<String>>> storeMap = new HashMap<>();
        for(ColumnDelete mutation: getColumnDeletes()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            String column = mutation.getColumnName();
            Map<String, List<String>> rowMap = storeMap.get(storeName);
            if(rowMap == null) {
                rowMap = new HashMap<>();
                storeMap.put(storeName, rowMap);
            }
            List<String> columnsList = rowMap.get(rowKey);
            if(columnsList == null) {
                columnsList = new ArrayList<>();
                rowMap.put(rowKey, columnsList);
            }
            columnsList.add(column);
        }
        return storeMap;
    }

    /**
     * Get the map of the row deletes as storeName -> list of row keys
     */
    public Map<String, List<String>> getRowDeletesMap() {
        Map<String, List<String>> storeMap = new HashMap<>();
        for(RowDelete mutation: getRowDeletes()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            List<String> rowList = storeMap.get(storeName);
            if(rowList == null) {
                rowList = new ArrayList<>();
                storeMap.put(storeName, rowList);
            }
            rowList.add(rowKey);
        }
        return storeMap;
    }
    
    //----- Column/row update methods
    
    /**
     * Add or set a column with the given binary value.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param columnName   Name of column.
     * @param columnValue  Column value in binary.
     */
    public void addColumn(String storeName, String rowKey, String columnName, byte[] columnValue) {
        m_columnUpdates.add(new ColumnUpdate(storeName, rowKey, columnName, columnValue));
    }

    /**
     * Add a column with empty value. The value will be empty byte array/empty string, not null
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param columnName   Name of column.
     */
    public void addColumn(String storeName, String rowKey, String columnName) {
        addColumn(storeName, rowKey, columnName, EMPTY);
    }
    
    /**
     * Add or set a column with the given string value. The column value is converted to
     * binary form using UTF-8.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param columnName   Name of column.
     * @param columnValue  Column value as a string.
     */
    public void addColumn(String storeName, String rowKey, String columnName, String columnValue) {
        addColumn(storeName, rowKey, columnName, Utils.toBytes(columnValue));
    }
    
    /**
     * Add or set a column with the given long value. The column value is converted to
     * binary form using Long.toString(colValue), which is then converted to a String using
     * UTF-8. 
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param columnName   Name of column.
     * @param columnValue  Column value as a long.
     */
    public void addColumn(String storeName, String rowKey, String columnName, long columnValue) {
        addColumn(storeName, rowKey, columnName, Long.toString(columnValue));
    }
    
    /**
     * Add an update that will delete the row with the given row key from the given store.
     * 
     * @param storeName Name of store from which to delete an object row.
     * @param rowKey    Row key in string form.
     */
    public void deleteRow(String storeName, String rowKey) {
        m_rowDeletes.add(new RowDelete(storeName, rowKey));
    }
    
    /**
     * Add an update that will delete the column for the given store, row key, and column
     * name. If a column update exists for the same store/row/column, the results are
     * undefined when the transaction is committed.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Row key in string form.
     * @param colName   Column name in string form.
     */
    public void deleteColumn(String storeName, String rowKey, String columnName) {
        m_columnDeletes.add(new ColumnDelete(storeName, rowKey, columnName));
    }

    /**
     * Add updates that will delete all given columns names for the given store name and
     * row key. If a column update exists for the same store/row/column, the results are
     * undefined when the transaction is committed.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Row key in string form.
     * @param columnNames  Collection of column names in string form.
     */
    public void deleteColumns(String storeName, String rowKey, Collection<String> columnNames) {
        for(String columnName: columnNames) {
            deleteColumn(storeName, rowKey, columnName);
        }
    }

    
    /**
     * For extreme logging.
     * 
     * @param logger  Logger to trace mutations to.
     */
    public void traceMutations(Logger logger) {
        logger.debug("Transaction in " + getTenant().getName() + ": " + getMutationsCount() + " mutations");
        for(ColumnUpdate mutation: getColumnUpdates()) {
            logger.trace(mutation.toString());
        }
        //2. delete columns
        for(ColumnDelete mutation: getColumnDeletes()) {
            logger.trace(mutation.toString());
        }
        //3. delete rows
        for(RowDelete mutation: getRowDeletes()) {
            logger.trace(mutation.toString());
        }
    }

    
}

