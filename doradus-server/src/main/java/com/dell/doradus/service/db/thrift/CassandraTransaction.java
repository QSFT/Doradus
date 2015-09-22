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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.ColumnDelete;
import com.dell.doradus.service.db.ColumnUpdate;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.RowDelete;

/**
 * Extends {@link DBTransaction} for Cassandra-specific transactions. Holds a map of column
 * update mutations, which can be accessed via {@link #getUpdateMap()}, and row deletions,
 * which can be accessed via {@link #getRowDeletionMap()}.  
 */
public class CassandraTransaction {
    private static final byte[] EMPTY_BYTES = new byte[0];

    //----- Cassandra-specific public methods
    
    public static Map<String, Set<ByteBuffer>> getRowDeletionMap(DBTransaction transaction) {
        Map<String, Set<ByteBuffer>> rowDeletionMap = new HashMap<>();
        for(RowDelete mutation: transaction.getRowDeletes()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            Set<ByteBuffer> rows = rowDeletionMap.get(storeName);
            if(rows == null) {
                rows = new HashSet<ByteBuffer>();
                rowDeletionMap.put(storeName, rows);
            }
            rows.add(Utils.toByteBuffer(rowKey));
        }
        return rowDeletionMap;
    }
    
    public static int totalColumnMutations(DBTransaction transaction) {
        return transaction.getColumnUpdates().size() + transaction.getColumnDeletes().size();
    }
    
    public static Map<ByteBuffer, Map<String, List<Mutation>>> getUpdateMap(DBTransaction transaction, long timestamp) {
        Map<ByteBuffer, Map<String, List<Mutation>>> updateMap = new HashMap<>();
        for(ColumnUpdate mutation: transaction.getColumnUpdates()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            DColumn column = mutation.getColumn();
            
            ByteBuffer rowKeyBB = Utils.toByteBuffer(rowKey);
            Map<String, List<Mutation>> colFamMap = updateMap.get(rowKeyBB);
            if (colFamMap == null) {
                colFamMap = new HashMap<String, List<Mutation>>();
                updateMap.put(rowKeyBB, colFamMap);
            }
            
            List<Mutation> mutationList = colFamMap.get(storeName);
            if (mutationList == null) {
                mutationList = new ArrayList<>();
                colFamMap.put(storeName, mutationList);
            }
            
            Mutation m = createMutation(Utils.toBytes(column.getName()), column.getRawValue(), timestamp);
            mutationList.add(m);
        }
        
        for(ColumnDelete mutation: transaction.getColumnDeletes()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            String column = mutation.getColumnName();
            
            ByteBuffer rowKeyBB = Utils.toByteBuffer(rowKey);
            Map<String, List<Mutation>> colFamMap = updateMap.get(rowKeyBB);
            if (colFamMap == null) {
                colFamMap = new HashMap<String, List<Mutation>>();
                updateMap.put(rowKeyBB, colFamMap);
            }
            
            List<Mutation> mutationList = colFamMap.get(storeName);
            if (mutationList == null) {
                mutationList = new ArrayList<>();
                colFamMap.put(storeName, mutationList);
            }
            
            Mutation m = createDeleteColumnMutation(Utils.toBytes(column), timestamp);
            mutationList.add(m);
        }
        
        return updateMap;
    }
    
    // Create a Mutation with the given column name and column value
    private static Mutation createMutation(byte[] colName, byte[] colValue, long timestamp) {
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
    }

    // Create a column mutation that deletes the given column name.
    private static Mutation createDeleteColumnMutation(byte[] colName, long timestamp) {
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.addToColumn_names(ByteBuffer.wrap(colName));
        
        Deletion deletion = new Deletion();
        deletion.setPredicate(slicePred);
        deletion.setTimestamp(timestamp);
        
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        return mutation;
    }

}
