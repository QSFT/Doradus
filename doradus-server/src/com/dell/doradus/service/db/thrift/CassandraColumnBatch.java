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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;

import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DColumn;

/**
 * Stores a batch of columns from a Cassandra database row. Implements Iterator&lt;DColumn&gt; 
 * so that columns can be examine via the {@link #hasNext()} and {@link #next()} methods.
 * When the current batch of columns has been exhausted and more columns may exist, a new
 * batch of columns is fetched from the database.
 */
public class CassandraColumnBatch implements Iterator<DColumn> {
    private String m_keyspace;
    private ColumnParent m_columnParent;
    private byte[] m_rowKey;
    private Iterator<ColumnOrSuperColumn> m_iColumns;
    private int m_sliceSize;
    private DColumn m_next = null;
    
    /**
     * Create a column batch from the given collection of initial columns. The keyspace,
     * source ColumnParent, and row key are given so that another batch can be fetched if
     * needed.
     * 
     * @param keyspace      Keyspace in which ColumnFamily resides.
     * @param colPar        ColumnParent where columns come from.
     * @param rowKey        Row key where columns come from.
     * @param firstColList  Initial set of columns fetched.
     */
    public CassandraColumnBatch(String keyspace, ColumnParent columnParent, byte[] rowKey,
                                List<ColumnOrSuperColumn> columns) {
        m_keyspace = keyspace;
        m_columnParent = columnParent;
        // Row key is taken from the key slice
        m_rowKey = rowKey;
        m_sliceSize = 0;
        m_iColumns = columns.iterator();
        // The slice is not empty, so we can simply get the first column
        shiftColumn();
    }   // constructor
    
    /**
     * Create a column batch and fetch the first set of columns using the given predicate.
     * If no columns are found for the given row key and slice predicate, the first call
     * to {@link #hasNext()} will return false.
     * 
     * @param thisConn         {@link DBConn} that provides the connection to perform the
     *                         fetch. It also tells us what keyspace to use for subsequent
     *                         column slices.
     * @param columnParent     Defines the column family to fetch columns from.
     * @param rowKey           Row of key to fetch columns from.
     * @param slicePredicate   Predicate that defines which columns to select.
     */
    public CassandraColumnBatch(DBConn thisConn, ColumnParent columnParent, byte[] rowKey,
                                SlicePredicate slicePredicate) {
        m_keyspace = thisConn.getKeyspace();
        m_columnParent = columnParent;
        m_rowKey = rowKey;
        List<ColumnOrSuperColumn> columns = thisConn.getSlice(m_columnParent, slicePredicate, ByteBuffer.wrap(m_rowKey));
        m_sliceSize = 0;
        m_iColumns = columns.iterator();
        if (!m_iColumns.hasNext()) {
            // tombstone? no columns to iterate
            return;
        }
        // The slice is not empty, so we can simply get the first column
        shiftColumn();
    }   // constructor
    
    @Override
    public boolean hasNext() {
        return m_next != null;
    }   // hasNext

    @Override
    public DColumn next() {
        if (m_next == null) {
            throw new NoSuchElementException("ColumnsIterator: No more column found");
        }
        DColumn result = m_next;
        shiftColumn();
        return result;
    }   // next

    @Override
    public void remove() {
        throw new UnsupportedOperationException("ColumnsIterator: Removing columns is not supported");
    }   // remove
    
    //----- Private methods
    
    /**
     * Provides retrieving of a next column.
     */
    private void shiftColumn() {
        if (m_iColumns.hasNext()) {
            // Current list may be used
            ColumnOrSuperColumn cosc = m_iColumns.next();
            m_sliceSize++;
            Column column = cosc.getColumn();
            m_next = new CassandraColumn(column.getName(), column.getValue());
        } else if (m_sliceSize < CassandraDefs.MAX_COLS_BATCH_SIZE) {
            // All columns were read; no sense to try to get more columns
            m_next = null;
            return;
        } else if (m_next != null) {
            // Save current column name
            String lastName = m_next.getName();
            List<ColumnOrSuperColumn> columns = getNextSlice(lastName);
            m_sliceSize = 0;
            m_iColumns = columns.iterator();
            if (!m_iColumns.hasNext()) {
                // column was deleted? We cannot iterate correctly...
                m_next = null;
                return;
            }
            // Normally the first column is the same as was the previous one.
            ColumnOrSuperColumn cosc = m_iColumns.next();
            m_sliceSize++;
            Column column = cosc.getColumn();
            m_next = new CassandraColumn(column.getName(), column.getValue());
            // Most probably we've got a column already read...
            // Shift from this column to the next one.
            if (lastName.equals(m_next.getName())) {
                // Just shift to next column
                shiftColumn();
            }
            // else the column was deleted.
            // We cannot guarantee that the iteration will continue correctly.
        }
    }   // shiftColumn
    
    private List<ColumnOrSuperColumn> getNextSlice(String lastName) {
        // TODO: Why aren't we passed the slide predicate? What if the column order is reversed?
        SlicePredicate slicePredicate = CassandraDefs.slicePredicateStartCol(Utils.toBytes(lastName));
        DBConn dbConn = ThriftService.instance().getDBConnection(m_keyspace);
        try {
            return dbConn.getSlice(m_columnParent, slicePredicate, ByteBuffer.wrap(m_rowKey));
        } finally {
            ThriftService.instance().returnDBConnection(dbConn);
        }
    }   // getNextSlice
}
