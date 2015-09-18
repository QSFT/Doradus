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
import java.util.List;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

/**
 * Contains static constants and routines shared by classes that access a Cassandra
 * database. All constants/methods are package-private.
 */
public final class CassandraDefs {
    // No member objects allowed.
    private CassandraDefs() { }

    //----- Constants
    
    /**
     * An zero-length byte[].
     */
    static final byte[] EMPTY_BYTES = new byte[0];
    
    /**
     * A ByteBuffer that wraps a zero-length byte[].
     */
    static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(EMPTY_BYTES);
    
    /**
     * The maximum number of columns fetched in a single slice.
     */
    static final int MAX_COLS_BATCH_SIZE = 10000;
    
    /**
     * The maximum number of rows fetched in a single batch.
     */
    static final int MAX_ROWS_BATCH_SIZE = 100;
    
    /**
     * A SliceRange that selects all columns, up to {@link #MAX_COLS_BATCH_SIZE}.
     */
    static final SliceRange SLICE_RANGE_ALL_COLS =
        new SliceRange(EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, CassandraDefs.MAX_COLS_BATCH_SIZE);
    
    /**
     * A SlicePredicate that selects all columns, up to {@link #MAX_COLS_BATCH_SIZE}.
     */
    static final SlicePredicate SLICE_PRED_ALL_COLS = new SlicePredicate();
    static {
        SLICE_PRED_ALL_COLS.setSlice_range(SLICE_RANGE_ALL_COLS);
    }
    
    /**
     * A KeyRange that selects all rows up to {@link #MAX_ROWS_BATCH_SIZE}.
     */
    static final KeyRange KEY_RANGE_ALL_ROWS = new KeyRange();
    static {
        KEY_RANGE_ALL_ROWS.setStart_key(EMPTY_BYTE_BUFFER);
        KEY_RANGE_ALL_ROWS.setEnd_key(EMPTY_BYTE_BUFFER);
        KEY_RANGE_ALL_ROWS.setCount(MAX_ROWS_BATCH_SIZE);
    }
    
    //----- static helper methods
    
    /**
     * Return the ColumnParent object for the given ColumnFamily name.
     * 
     * @param cfName    Name of a ColumnFamily.
     * @return          ColumnParent object for the given CF name.
     */
    static ColumnParent columnParent(String cfName) {
        return new ColumnParent(cfName);
    }   // columnParent
    
    /**
     * Convert the given row keys from byte[]s to a list of ByteBuffers.
     * 
     * @param rowKeys   Collection of row keys as byte[]s.
     * @return          List of BufferBuffer that wrap the given byte[]s.
     */
    static List<ByteBuffer> convertByteKeys(Collection<byte[]> rowKeys) {
        List<ByteBuffer> rowKeyList = new ArrayList<ByteBuffer>();
        for (byte[] rowKey : rowKeys) {
            rowKeyList.add(ByteBuffer.wrap(rowKey));
        }
        return rowKeyList;
    }   // convertByteKeys
    
    /**
     * Create a KeyRange that begins at the given row key.
     * 
     * @param startRowKey   Starting row key as a byte[].
     * @return              KeyRange that starts at the given row, open-ended.
     */
    static KeyRange keyRangeStartRow(byte[] startRowKey) {
        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(startRowKey);
        keyRange.setEnd_key(EMPTY_BYTE_BUFFER);
        keyRange.setCount(MAX_ROWS_BATCH_SIZE);
        return keyRange;
    }   // keyRangeStartRow

    static KeyRange keyRangeStartRow(byte[] startRowKey, int count) {
        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(startRowKey == null ? EMPTY_BYTES : startRowKey);
        keyRange.setEnd_key(EMPTY_BYTES);
        keyRange.setCount(count);
        return keyRange;
    }
    
    /**
     * Create a KeyRange that selects a single row with the given key.
     * 
     * @param rowKey    Row key as a byte[].
     * @return          KeyRange that starts and ends with the given key.
     */
    static KeyRange keyRangeSingleRow(byte[] rowKey) {
        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(rowKey);
        keyRange.setEnd_key(rowKey);
        keyRange.setCount(1);
        return keyRange;
    }   // keyRangeSingleRow
    
    /**
     * Create a SlicePredicate that starts at the given column name, selecting up to
     * {@link #MAX_COLS_BATCH_SIZE} columns.
     * 
     * @param startColName  Starting column name as a byte[].
     * @return              SlicePredicate that starts at the given column name,
     *                      open-ended, selecting up to {@link #MAX_COLS_BATCH_SIZE}
     *                      columns.
     */
    static SlicePredicate slicePredicateStartCol(byte[] startColName) {
        if(startColName == null) startColName = EMPTY_BYTES;
        SliceRange sliceRange =
            new SliceRange(ByteBuffer.wrap(startColName), EMPTY_BYTE_BUFFER, false, CassandraDefs.MAX_COLS_BATCH_SIZE);
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.setSlice_range(sliceRange);
        return slicePred;
    }   // slicePredicateStartCol

    /**
     * Create a SlicePredicate that starts at the given column name, selecting up to
     * {@link #MAX_COLS_BATCH_SIZE} columns.
     * 
     * @param startColName  Starting column name as a byte[].
     * @param endColName    Ending column name as a byte[]
     * @return              SlicePredicate that starts at the given starting column name,
     *                      ends at the given ending column name, selecting up to
     *                      {@link #MAX_COLS_BATCH_SIZE} columns.
     */
    static SlicePredicate slicePredicateStartEndCol(byte[] startColName, byte[] endColName, int count) {
        if(startColName == null) startColName = EMPTY_BYTES;
        if(endColName == null) endColName = EMPTY_BYTES;
        SliceRange sliceRange =
            new SliceRange(ByteBuffer.wrap(startColName), ByteBuffer.wrap(endColName), false, count);
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.setSlice_range(sliceRange);
        return slicePred;
    }   // slicePredicateStartCol
    
    /**
     * Create a SlicePredicate that starts at the given column name, selecting up to
     * {@link #MAX_COLS_BATCH_SIZE} columns.
     * 
     * @param startColName  Starting column name as a byte[].
     * @param endColName	Ending column name as a byte[]
     * @return              SlicePredicate that starts at the given starting column name,
     *                      ends at the given ending column name, selecting up to
     *                      {@link #MAX_COLS_BATCH_SIZE} columns.
     */
    static SlicePredicate slicePredicateStartEndCol(byte[] startColName, byte[] endColName, boolean reversed) {
        if(startColName == null) startColName = EMPTY_BYTES;
        if(endColName == null) endColName = EMPTY_BYTES;
        SliceRange sliceRange =
            new SliceRange(
            		ByteBuffer.wrap(startColName), ByteBuffer.wrap(endColName), 
            		reversed, CassandraDefs.MAX_COLS_BATCH_SIZE);
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.setSlice_range(sliceRange);
        return slicePred;
    }   // slicePredicateStartCol
    
    /**
     * Create a SlicePredicate that starts at the given column name, selecting up to
     * {@link #MAX_COLS_BATCH_SIZE} columns.
     * 
     * @param startColName  Starting column name as a byte[].
     * @param endColName	Ending column name as a byte[]
     * @return              SlicePredicate that starts at the given starting column name,
     *                      ends at the given ending column name, selecting up to
     *                      {@link #MAX_COLS_BATCH_SIZE} columns.
     */
    static SlicePredicate slicePredicateStartEndCol(byte[] startColName, byte[] endColName) {
        return slicePredicateStartEndCol(startColName, endColName, false);
    }   // slicePredicateStartCol
    
    /**
     * Create a SlicePredicate that selects a single column.
     * 
     * @param colName   Column name as a byte[].
     * @return          SlicePredicate that select the given column name only.
     */
    static SlicePredicate slicePredicateColName(byte[] colName) {
        SlicePredicate slicePred = new SlicePredicate();
        slicePred.addToColumn_names(ByteBuffer.wrap(colName));
        return slicePred;
    }   // slicePredicateColName
    
    /**
     * Create a SlicePredicate that selects the given column names.
     * 
     * @param colNames  A collection of column names as byte[]s.
     * @return          SlicePredicate that selects the given column names only.
     */
    static SlicePredicate slicePredicateColNames(Collection<byte[]> colNames) {
        SlicePredicate slicePred = new SlicePredicate();
        for (byte[] colName : colNames) {
            slicePred.addToColumn_names(ByteBuffer.wrap(colName));
        }
        return slicePred;
    }   // slicePredicateColNames
    
}   // CassandraDefs
