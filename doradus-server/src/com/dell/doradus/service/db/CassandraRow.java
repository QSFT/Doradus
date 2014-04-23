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

import java.util.Iterator;

import com.dell.doradus.common.Utils;

/**
 * Implements a Doradus {@link DRow} fetched from a Cassandra ColumnFamily row.
 */
public class CassandraRow implements DRow {
    private final String m_rowKey;
    private final CassandraColumnBatch m_colBatch;

    public CassandraRow(byte[] rowKey, CassandraColumnBatch colBatch) {
        m_rowKey = Utils.toString(rowKey);
        m_colBatch = colBatch;
    }   // constructor
    
    @Override
    public String getKey() {
        return m_rowKey;
    }   // getKey

    @Override
    public Iterator<DColumn> getColumns() {
        return m_colBatch;
    }   // getColumns

}   // class CassandraRow
