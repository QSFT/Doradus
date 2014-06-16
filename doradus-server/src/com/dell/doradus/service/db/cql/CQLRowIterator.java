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
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;

/**
 * Iterates through rows in a CQL dynamic table and returns each logical row. Assumes 
 * the following CQL table structure:
 * <pre>
 *      CREATE TABLE foo (
 *          key     text,
 *          column1 text,
 *          value   text,   // or blob
 *          PRIMARY KEY(key, column1)
 *      )
 * </pre>
 * Each logical (Doradus) row consists of all CQL rows in the table above with the same
 * key. Each CQL row stores one column, whose name lives in "column1" and whose value
 * lives in "value". CQLRowIterator iterates through the rows in the given ResultSet and
 * bundles them into logical rows.
 */
public class CQLRowIterator implements Iterator<DRow> {
    private final ResultSet m_rs;
    private       Row       m_nextRow;
    private       boolean   m_bValueIsBinary;

    /**
     * Create a row iterator that extracts rows from the given CQL ResultSet.
     * 
     * @param rs    ResultSet created by a CQL query.
     */
    public CQLRowIterator(ResultSet rs) {
        m_rs = rs;
        m_nextRow = rs.one();
        setColumnValueType();
    }

    @Override
    public boolean hasNext() {
        return m_nextRow != null;
    }

    @Override
    public DRow next() {
        Utils.require(m_nextRow != null, "No more rows");
        
        // Fetch all columns for this row in case next() is called before they are consumed.
        List<DColumn> columnList = new ArrayList<>();
        String key = m_nextRow.getString("key");
        do {
            if (m_bValueIsBinary) {
                columnList.add(new CQLColumn(m_nextRow.getString("column1"), m_nextRow.getBytes("value")));
            } else {
                columnList.add(new CQLColumn(m_nextRow.getString("column1"), m_nextRow.getString("value")));
            }
            m_nextRow = m_rs.one();
        } while (m_nextRow != null && m_nextRow.getString("key").equals(key));
        return new CQLRow(key, columnList.iterator());
    }

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
    
}   // class CQLRowIterator
