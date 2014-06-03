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

package com.dell.doradus.service.db.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.slf4j.Logger;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.Defs;
import com.dell.doradus.service.db.DBTransaction;

/**
 * Extends {@link DBTransaction} for Cassandra-specific transactions. Holds a map of column
 * update mutations, which can be accessed via {@link #getUpdateMap()}, and row deletions,
 * which can be accessed via {@link #getRowDeletionMap()}.  
 */
public class CassandraTransaction extends DBTransaction {
    private static final byte[] EMPTY_BYTES = new byte[0];

    // Timestamp used for all updates: 
    private final long m_timestamp;

    // Row key -> ColumnFamily -> Mutation list
    private final Map<ByteBuffer, Map<String, List<Mutation>>> m_mutationMap = new HashMap<>();
    
    // ColumnFamily -> key row deletion map.
    private final Map<String, Set<ByteBuffer>> m_deletionMap = new HashMap<>();

    // Total column updates and row deletes so far.
    private int m_colUpdates;
    private int m_rowDeletes;

    /**
     * Create a new transaction with a timestamp of "now".
     */
    public CassandraTransaction() {
        m_timestamp = Utils.getTimeMicros();
    }   // constructor

    //----- General methods
    
    /**
     * Clear all mutations from this map.
     */
    @Override
    public void clear() {
        m_mutationMap.clear();
        m_deletionMap.clear();
        m_colUpdates = 0;
        m_rowDeletes = 0;
    }   // clear
    
    /**
     * The number of updates stores so far. This includes column mutations and row deletes.
     * 
     * @return  The total number of updates stored so far.
     */
    @Override
    public int getUpdateCount() {
        return m_colUpdates + m_rowDeletes;
    }   // getUpdateCount
    
    //----- DBTransaction: Application schema update methods 
    
    @Override
    public void addAppColumn(String appName, String colName, String colValue) {
        Mutation mutation = createMutation(Utils.toBytes(colName), Utils.toBytes(colValue), m_timestamp);
        addMutation(DBConn.COLUMN_FAMILY_APPS, appName, mutation);
    }   // addAppColumn

    @Override
    public void deleteAppRow(String appName) {
        deleteRow(DBConn.COLUMN_FAMILY_APPS, appName);
    }   // deleteAppRow

    @Override
    public void setDBOption(String optName, String optValue) {
        Mutation mutation = createMutation(Utils.toBytes(optName), Utils.toBytes(optValue), m_timestamp);
        addMutation(DBConn.COLUMN_FAMILY_APPS, Defs.OPTIONS_ROW_KEY, mutation);
    }   // setDBOption

    //----- DBTransaction: Column/row update methods

    @Override
    public void addColumn(String storeName, String rowKey, String colName, byte[] colValue) {
        Mutation mutation = createMutation(Utils.toBytes(colName), colValue, m_timestamp);
        addMutation(storeName, rowKey, mutation);
    }   // addColumn
    
    @Override
    public void addColumn(String storeName, String rowKey, String colName, long colValue) {
        Mutation mutation = createMutation(Utils.toBytes(colName), Utils.toBytes(Long.toString(colValue)), m_timestamp);
        addMutation(storeName, rowKey, mutation);
    }   // addColumn
    
    @Override
    public void deleteColumn(String storeName, String rowKey, String colName) {
        Mutation mutation = createDeleteColumnMutation(Utils.toBytes(colName));
        addMutation(storeName, rowKey, mutation);
    }   // deleteColumn

    @Override
    public void deleteColumns(String storeName, String rowKey, Collection<String> colNames) {
    	List<ByteBuffer> colNamesList = new ArrayList<>();
    	for (String colName : colNames) {
    		colNamesList.add(ByteBuffer.wrap(Utils.toBytes(colName)));
    	}
        Mutation mutation = createDeleteColumnMutation(colNamesList);
        addMutation(storeName, rowKey, mutation);
    }   // deleteColumn

    @Override
    public void deleteRow(String storeName, String objectID) {
        Set<ByteBuffer> keySet = m_deletionMap.get(storeName);
        if (keySet == null) {
            keySet = new HashSet<>();
            m_deletionMap.put(storeName, keySet);
        }
        keySet.add(ByteBuffer.wrap(Utils.toBytes(objectID)));
        m_rowDeletes++;
    }   // deleteObjectRow

    //----- Cassandra-specific public methods
    
    /**
     * Return the transaction timestamp used by this object. It is set when the object is
     * created.
     * 
     * @return  The transaction timestamp used by this object.
     */
    public long getTimestamp() {
        return m_timestamp;
    }   // getTimestamp
    
    /**
     * Get the row deletion map, which contains ColumnFamily to row keys to be deleted.
     * The map will be empty if {@link #deleteRow(String, String)} was not called since
     * this transaction was created or cleared.
     * 
     * @return  The row deletion map.
     */
    public Map<String, Set<ByteBuffer>> getRowDeletionMap() {
        return m_deletionMap;
    }   // getRowDeletionMap
    
    /**
     * Get the internal column mutation map. The map uses the Cassandra/Thrift format:
     * <pre>
     *      {row key} -> {ColumnFamily name} -> {Mutation list}
     * </pre>
     * The map is not copied, so the caller must not modify!
     * 
     * @return  The internal mutation map.
     */
    public Map<ByteBuffer, Map<String, List<Mutation>>> getUpdateMap() {
        return m_mutationMap;
    }   // getUpdateMap
    
    /**
     * Return the total number of column mutations pending for this transaction.
     * 
     * @return  The total number of column mutations pending for this transaction.
     */
    public int totalColumnMutations() {
        return m_colUpdates;
    }   // totalColumnMutations
    
    /**
     * Return the total number of row deletes pending for this transaction.
     * 
     * @return  The total number of row deletes pending for this transaction.
     */
    public int totalRowDeletes() {
        return m_rowDeletes;
    }   // totalRowDeletes
    
    /**
     * For extreme logging.
     * 
     * @param logger  Logger to trace mutations to.
     */
    public void traceMutations(Logger logger) {
        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mapEntry : m_mutationMap.entrySet()) {
            ByteBuffer rowKey = mapEntry.getKey();
            Map<String, List<Mutation>> colFamMap = mapEntry.getValue(); 
            for (String colFam : colFamMap.keySet()) {
                List<Mutation> mutationList = colFamMap.get(colFam);
                logger.trace("{}['{}'] has {} column mutations:",
                               new Object[]{colFam, Utils.deWhite(rowKey), mutationList.size()});
                for (Mutation mutation : mutationList) {
                    // What kind of mutation is it?
                    if (mutation.isSetDeletion()) {
                        // Deletion
                        Deletion deletion = mutation.getDeletion();
                        SlicePredicate predicate = deletion.getPredicate();
                        if (predicate.isSetColumn_names()) {
                            List<ByteBuffer> colNames = predicate.getColumn_names();
                            StringBuilder buffer = new StringBuilder();
                            for (ByteBuffer colName : colNames) {
                                if (buffer.length() > 0) {
                                    buffer.append(", ");
                                }
                                buffer.append(Utils.deWhite(colName));
                            }
                            logger.trace("   Deleted: {}", buffer.toString());
                        } else {
                            SliceRange sliceRange = predicate.getSlice_range();
                            logger.trace("   Deleted: {} to {}",
                                           Utils.deWhite(sliceRange.getStart()),
                                           Utils.deWhite(sliceRange.getFinish()));
                        }
                    } else {
                        ColumnOrSuperColumn cosc = mutation.getColumn_or_supercolumn();
                        if (cosc.isSetCounter_column()) {
                            // Counter column
                            CounterColumn counterCol = cosc.getCounter_column();
                            logger.trace("   {} += ({})",
                                           Utils.deWhite(counterCol.getName()),
                                           counterCol.getValue());
                        } else if (cosc.isSetColumn()) {
                            // Regular column
                            Column col = cosc.getColumn();
                            String colValue = Utils.deWhite(col.getValue());
                            if (colValue == null || colValue.length() == 0) 
                                colValue = "[null]";
                            logger.trace("   {} : {}", Utils.deWhite(col.getName()), colValue);
                        } else if (cosc.isSetSuper_column()) {
                            // Supercolumn: since we don't expect these, just print the name.
                            SuperColumn superCol = cosc.getSuper_column();
                            logger.trace("   Supercolumn '{}' set", Utils.deWhite(superCol.getName()));
                            
                        }
                    }
                }   // for mutation list
            }   // for column family map
        }   // for row key
    }   // traceMutations
    
    public void addMutationList(String cfName, String rowKey, List<Mutation> mutations) {
        ByteBuffer rowKeyBB = ByteBuffer.wrap(Utils.toBytes(rowKey));
        Map<String, List<Mutation>> colFamMap = m_mutationMap.get(rowKeyBB);
        if (colFamMap == null) {
            colFamMap = new HashMap<String, List<Mutation>>();
            m_mutationMap.put(rowKeyBB, colFamMap);
        }
        
        List<Mutation> mutationList = colFamMap.get(cfName);
        if (mutationList == null) {
            mutationList = new ArrayList<>();
            colFamMap.put(cfName, mutationList);
        }
        
        mutationList.addAll(mutations);
        m_colUpdates += mutations.size();
    }
    
    //----- Private methods

    // Add the given mutation to the column mutation map.
    private void addMutation(String cfName, String rowKey, Mutation mutation) {
        ByteBuffer rowKeyBB = ByteBuffer.wrap(Utils.toBytes(rowKey));
        Map<String, List<Mutation>> colFamMap = m_mutationMap.get(rowKeyBB);
        if (colFamMap == null) {
            colFamMap = new HashMap<String, List<Mutation>>();
            m_mutationMap.put(rowKeyBB, colFamMap);
        }
        
        List<Mutation> mutationList = colFamMap.get(cfName);
        if (mutationList == null) {
            mutationList = new ArrayList<>();
            colFamMap.put(cfName, mutationList);
        }
        
        mutationList.add(mutation);
        m_colUpdates++;
    }   // addMutation

    // Create a Mutation with the given column name, value, and timestamp
    private Mutation createMutation(byte[] colName, byte[] colValue, long timestamp) {
        if (colValue == null) {
            colValue = EMPTY_BYTES;
        }
        Column col = new Column();
        col.setName(colName);
        col.setValue(colValue);
        col.setTimestamp(timestamp);
        
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(col);
        
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        return mutation;
    }   // createMutation

    private Mutation createDeleteColumnMutation(byte[] colName) {
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.addToColumn_names(ByteBuffer.wrap(colName));
        
        Deletion deletion = new Deletion();
        deletion.setPredicate(slicePred);
        deletion.setTimestamp(m_timestamp);
        
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        return mutation;
    }   // createDeleteColumnMutation

    private Mutation createDeleteColumnMutation(List<ByteBuffer> colNames) {
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.setColumn_names(colNames);
        
        Deletion deletion = new Deletion();
        deletion.setPredicate(slicePred);
        deletion.setTimestamp(m_timestamp);
        
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        return mutation;
    }   // createDeleteColumnMutation
    
}   // MutationMap
