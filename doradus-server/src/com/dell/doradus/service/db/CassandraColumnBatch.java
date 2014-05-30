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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;

import com.dell.doradus.common.Utils;

/**
 * Stores a batch of columns from a Cassandra database row. Implements Iterator&lt;DColumn&gt; 
 * so that columns can be examine via the {@link #hasNext()} and {@link #next()} methods.
 * When the current batch of columns has been exhausted and more columns may exist, a new
 * batch of columns is fetched from the database.
 */
public class CassandraColumnBatch implements Iterator<DColumn> {
	// Cassandra Column Family representative
	ColumnParent m_columnParent;
	// A range of a single key
	byte[] m_rowKey;
	// Current slice (columns list) iterator
	Iterator<ColumnOrSuperColumn> m_iColumns;
	// Size of the current slice
	int m_sliceSize;
	
	// A column that should be yielded as a next column.
	DColumn m_next = null;
	
    /**
     * Create a column batch from the given collection of initial columns. The source
     * ColumnParent and row key are given so that another batch can be fetched if needed.
     * 
     * @param colPar        ColumnParent where columns come from.
     * @param rowKey        Row key where columns come from.
     * @param firstColList  Initial set of columns fetched.
     */
	CassandraColumnBatch(ColumnParent columnParent, byte[] rowKey, List<ColumnOrSuperColumn> columns) {
		m_columnParent = columnParent;
		// Row key is taken from the key slice
		m_rowKey = rowKey;
		m_sliceSize = 0;
		m_iColumns = columns.iterator();
		// The slice is not empty, so we can simply get the first column
		shiftColumn();
	}
	
	private CassandraColumnBatch(CassandraDBConn thisConn, ColumnParent columnParent,
            byte[] rowKey, SlicePredicate slicePredicate) {
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
	}	// constructor
	
	CassandraColumnBatch(CassandraDBConn thisConn, ColumnParent columnParent,
			             byte[] rowKey, byte[] startCol, byte[] endCol, boolean reversed) {
		this(thisConn, columnParent, rowKey, CassandraDefs.slicePredicateStartEndCol(startCol, endCol, reversed));
	}	// constructor
	
	CassandraColumnBatch(CassandraDBConn thisConn, ColumnParent columnParent,
			byte[] rowKey, byte[] startCol, byte[] endCol) {
		this(thisConn, columnParent, rowKey, CassandraDefs.slicePredicateStartEndCol(startCol, endCol));
	}	// constructor

    /**
     * Create a column batch by fetching an initial set of columns for the given row key
     * from the given ColumnParent, using the given database connection. If the given row
     * does not exist, {@link #hasNext()} will immediately return false. Otherwise,
     * columns can be retrieved via {@link #next()}, and additional batches will be fetched
     * as needed using a new database connection.
     * 
     * @param dbConn        {@link CassandraDBConn} to use to fetch first column batch
     *                      (not saved -- only used by the constructor).
     * @param colPar        ColumnParent where columns come from.
     * @param rowKey        Row key where columns come from.
     */
	CassandraColumnBatch(CassandraDBConn thisConn, ColumnParent columnParent, byte[] rowKey) {
		this(thisConn, columnParent, rowKey, CassandraDefs.SLICE_PRED_ALL_COLS);
	}	// constructor

	@Override
	public boolean hasNext() {
		return m_next != null;
	}	// hasNext

	@Override
	public DColumn next() {
		if (m_next == null) {
			throw new NoSuchElementException("ColumnsIterator: No more column found");
		}
		DColumn result = m_next;
		shiftColumn();
		return result;
	}	// next

	@Override
	public void remove() {
		throw new UnsupportedOperationException("ColumnsIterator: Removing columns is not supported");
	}	// remove
	
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
	}	// shiftColumn
	
	private List<ColumnOrSuperColumn> getNextSlice(String lastName) {
		SlicePredicate slicePredicate = CassandraDefs.slicePredicateStartCol(Utils.toBytes(lastName));
        CassandraDBConn dbConn = (CassandraDBConn)DBService.instance().getDBConnection();
        try {
        	return dbConn.getSlice(m_columnParent, slicePredicate, ByteBuffer.wrap(m_rowKey));
        } finally {
        	DBService.instance().returnDBConnection(dbConn);
        }
	}	// getNextSlice
}
