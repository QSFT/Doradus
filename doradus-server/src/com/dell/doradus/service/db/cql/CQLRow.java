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

import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;

/**
 * Holds a {@link DRow} extracted from a CQL dynamic table. A row consists of all
 * {@link DColumn}s with the same row key.
 */
public class CQLRow implements DRow {
    private final String            m_key;
    private final Iterator<DColumn> m_colIter;

    /**
     * Create a row with the given key that will return columns via the given iterator.
     * 
     * @param key       Row key.
     * @param colIter   Iterator that returns all columns belonging to this row.
     */
    public CQLRow(String key, Iterator<DColumn> colIter) {
        m_key = key;
        m_colIter = colIter;
    }
    
    @Override
    public String getKey() {
        return m_key;
    }

    @Override
    public Iterator<DColumn> getColumns() {
        return m_colIter;
    }

}   // class CQLRow
