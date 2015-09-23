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

import java.util.List;

public class ColumnSequence implements Sequence<DColumn> {
    private DRow m_row;
    private String m_startColumn;
    private String m_endColumn;
    private int m_chunkSize;
    private List<DColumn> m_currentList;
    private int m_pointer;

    public ColumnSequence(DRow row, String startColumn, String endColumn, int chunkSize) {
        m_row = row;
        m_startColumn = startColumn;
        m_endColumn = endColumn;
        m_chunkSize = chunkSize;
    }
    
    @Override public DColumn next() {
        if(m_currentList == null) {
            m_currentList = DBService.instance().getColumns(m_row.getNamespace(), m_row.getStoreName(), m_row.getRowKey(),
                    m_startColumn, m_endColumn, m_chunkSize);
        }
        if(m_currentList.size() == 0) {
            return null;
        }
        if(m_pointer == m_currentList.size()) {
            String newStartKey = m_currentList.get(m_pointer - 1).getName() + '\0';
            m_currentList = DBService.instance().getColumns(m_row.getNamespace(), m_row.getStoreName(), m_row.getRowKey(),
                    newStartKey, m_endColumn, m_chunkSize);
            m_pointer = 0;
        }
        if(m_currentList.size() == 0) {
            return null;
        }
        return m_currentList.get(m_pointer++);
    }
}
