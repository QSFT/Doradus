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

import java.util.Iterator;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DColumn;

/**
 * Iterates through rows in a CQL dynamic table and returns each logical column. Assumes 
 * the CQL table structure:
 * <pre>
 *      CREATE TABLE foo (
 *          key     text,
 *          column1 text,
 *          value   text,   // or blob
 *          PRIMARY KEY(key, column1)
 *      )
 * </pre>
 * Each CQL row holds one logical column whose name is stored in "column1" and whose value
 * is stored in "value". Each logical column1/value pair is returned as a {@link DColumn}.
 * <p>
 * This iterator can be used with a ResultSet that is iterating through rows with different
 * keys. The iterator stops if a CQL row is found whose key is not the same as the first one
 * found.
 */
public class CQLColumnIterator implements Iterator<DColumn> {
    private final ResultSet m_rs;
    private       Row       m_nextRow;
    private final String    m_key;
    private       boolean   m_bValueIsBinary;
    
    /**
     * Construct a new column iterator using the given ResultSet.
     * 
     * @param rs    ResultSet from which to read rows.
     */
    public CQLColumnIterator(ResultSet rs) {
        m_rs = rs;
        m_nextRow = rs.one();
        m_key = m_nextRow == null ? "" : m_nextRow.getString("key");
        setColumnValueType();
    }

    @Override
    public boolean hasNext() {
        return m_nextRow != null;
    }

    @Override
    public DColumn next() {
        Utils.require(m_nextRow != null, "No more columns to return"); 
        CQLColumn cqlCol = null;
        if (m_bValueIsBinary) {
            cqlCol = new CQLColumn(m_nextRow.getString("column1"), m_nextRow.getBytes("value"));
        } else {
            cqlCol = new CQLColumn(m_nextRow.getString("column1"), m_nextRow.getString("value"));
        }
        m_nextRow = m_rs.one();
        if (m_nextRow != null && !m_key.equals(m_nextRow.getString("key"))) {
            m_nextRow = null;
        }
        return cqlCol;
    }   // next

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    // Determine if column value is binary based on first row's column metadata.
    private void setColumnValueType() {
        if (m_nextRow != null) {
            ColumnDefinitions colDefs = m_nextRow.getColumnDefinitions();
            DataType dataType = colDefs.getType("value");
            m_bValueIsBinary = dataType.equals(DataType.blob());
        }
    }   // setColumnValueType
    
}   // class CQLColumnIterator
