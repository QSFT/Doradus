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

package com.dell.doradus.db.dynamodb;

import java.util.Iterator;

import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;

/**
 * Implements {@link DRow} for the DynamoDB service. Stores the row key and
 * {@link DDBColumnIterator} for a row.
 */
public class DDBRow implements DRow {
    private final String            m_key;
    private final DDBColumnIterator m_ddbColIter;

    public DDBRow(String rowKey, DDBColumnIterator colIter) {
        m_key = rowKey;
        m_ddbColIter = colIter;
    }
    
    @Override
    public String getKey() {
        return m_key;
    }

    @Override
    public Iterator<DColumn> getColumns() {
        return m_ddbColIter;
    }

    @Override
    public String toString() {
        return "Row [" + m_key + "]: " + m_ddbColIter.toString();
    }
    
    public String toVerboseString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("Row [" + m_key + "]: ");
        buffer.append(m_ddbColIter.toVerboseString());
        return buffer.toString();
    }
    
}   // class DDBRow
