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
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.Tenant;

/**
 * Extends {@link DBTransaction} for Cassandra-specific transactions. Holds a map of column
 * update mutations, which can be accessed via {@link #getUpdateMap()}, and row deletions,
 * which can be accessed via {@link #getRowDeletionMap()}.  
 */
public class CassandraTransaction extends DBTransaction {
    private static final byte[] EMPTY_BYTES = new byte[0];

    // Timestamp used for all updates: 
    private final long m_timestamp;

    /**
     * Create a new transaction with a timestamp of "now".
     */
    public CassandraTransaction(Tenant tenant) {
        super(tenant.getKeyspace());
        m_timestamp = Utils.getTimeMicros();
    }

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

    // Create a Mutation with the given column name and column value
    private Mutation createMutation(byte[] colName, byte[] colValue) {
        if (colValue == null) {
            colValue = EMPTY_BYTES;
        }
        Column col = new Column();
        col.setName(colName);
        col.setValue(colValue);
        col.setTimestamp(m_timestamp);
        
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(col);
        
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        return mutation;
    }

    // Create a column mutation that deletes the given column name.
    private Mutation createDeleteColumnMutation(byte[] colName) {
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.addToColumn_names(ByteBuffer.wrap(colName));
        
        Deletion deletion = new Deletion();
        deletion.setPredicate(slicePred);
        deletion.setTimestamp(m_timestamp);
        
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        return mutation;
    }

}
