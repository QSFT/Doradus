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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;

/**
 * Stores a batch of rows from a Cassandra database store. Implements Iterator&lt;DRow&gt;
 * so that {@link #hasNext()} and {@link #next()} can be used to retrieve rows. When the
 * current batch of rows is exhausted, another batch of rows is fetched as needed. 
 */
public class CassandraRowBatch implements Iterator<DRow> {

    // Cassandra Column Family representative
    ColumnParent m_columnParent;
    // Current length of key range
    int m_countRows;
    // Last row key that was read
    byte[] m_lastKey;
    
    // Current key range iterator (from a list of KeySlice objects)
    Iterator<KeySlice> m_iRows;
    // Next Row that will be yielded next time when a next row is requested.
    DRow m_next = null;
    
    /**
     * Create a row batch by retrieving all columns of the first batch of rows (starting
     * at key 0x0) from the given ColumnParent. The given database is used for the fetch,
     * but it is not retained -- a new connection is used as needed. If there are no rows
     * in the given store, {@link #hasNext()} will return false immediately.
     * 
     * @param dbConn        {@link CassandraDBConn} to use for initial fetch (not saved --
     *                      only used by the constructor).
     * @param columnParent  Cassandra Column Family to fetch from.
     */
    public CassandraRowBatch(CassandraDBConn dbConn, ColumnParent columnParent) {
        m_columnParent = columnParent;
        KeyRange keyRange = CassandraDefs.KEY_RANGE_ALL_ROWS;
        SlicePredicate slicePredicate = CassandraDefs.SLICE_PRED_ALL_COLS;
        List<KeySlice> keySliceList = dbConn.getRangeSlices(columnParent, slicePredicate, keyRange);
        m_countRows = 0;
        m_iRows = keySliceList.iterator();
        m_lastKey = CassandraDefs.EMPTY_BYTES;
        
        // Get a first row
        shiftRow();
    }   // constructor

    @Override
    public boolean hasNext() {
        return m_next != null;
    }

    @Override
    public DRow next() {
        if (m_next == null) {
            throw new NoSuchElementException("RowsIterator: No more rows");
        }
        DRow result = m_next;
        shiftRow();
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("RowsIterator: Removing rows is not supported");
    }
    
    private KeySlice nextSlice() {
    	m_countRows++;
    	return m_iRows.next();
    }
    
    /**
     * Provides retrieving of a next row.
     */
    private void shiftRow() {
    	for (;;) {
	        if (m_iRows.hasNext()) {
	            // A next row is taken from the current List iterator.
	            KeySlice keySlice = nextSlice();
	            m_lastKey = keySlice.getKey();
	            List<ColumnOrSuperColumn> columnsList = keySlice.getColumns();
	            if (!columnsList.isEmpty()) {
	                m_next = new CassandraRow(m_lastKey, new CassandraColumnBatch(m_columnParent, m_lastKey, columnsList));
	                break;
	            }
	        } else if (m_countRows < CassandraDefs.MAX_ROWS_BATCH_SIZE) {
	            // No more rows
	            m_next = null;
	            break;
	        } else {
	            List<KeySlice> keySliceList = getNextBatch(m_lastKey);
	            m_countRows = 0;
	            m_iRows = keySliceList.iterator();
	            if (m_iRows.hasNext()) {
	                KeySlice keySlice = nextSlice();
	                List<ColumnOrSuperColumn> columnsList = keySlice.getColumns();
	                // Most probably it is the same last row...
	                boolean equalKeys = Arrays.equals(m_lastKey, keySlice.getKey());
	                m_lastKey = keySlice.getKey();
	                if (!equalKeys && !columnsList.isEmpty()) {
	                    m_next = new CassandraRow(m_lastKey, new CassandraColumnBatch(m_columnParent, m_lastKey, columnsList));
	                    break;
	                }
	            } else {
	                // Last row is lost? Strange... Anyway no more rows exist.
	                m_next = null;
	                break;
	            }
	        }
    	}
    }   // shiftRow
    
    private List<KeySlice> getNextBatch(byte[] startKey) {
        KeyRange keyRange = CassandraDefs.keyRangeStartRow(startKey);
        SlicePredicate slicePredicate = CassandraDefs.SLICE_PRED_ALL_COLS;
        CassandraDBConn dbConn = (CassandraDBConn)DBService.instance().getDBConnection();
        try {
            return dbConn.getRangeSlices(m_columnParent, slicePredicate, keyRange);
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }   // getNextBatch
    
}   // class CassandraRowBatch
