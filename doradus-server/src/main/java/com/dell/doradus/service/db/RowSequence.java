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

public class RowSequence implements Sequence<DRow> {
    private Tenant m_tenant;
    private String m_storeName;
    private int m_chunkSize;
    private List<String> m_currentList;
    private int m_pointer;

    public RowSequence(Tenant tenant, String storeName, int chunkSize) {
        m_tenant = tenant;
        m_storeName = storeName;
        m_chunkSize = chunkSize;
    }
    
    @Override public DRow next() {
        if(m_currentList == null) {
            m_currentList = DBService.instance(m_tenant).getRows(m_storeName, null, m_chunkSize);
        }
        if(m_currentList.size() == 0) {
            return null;
        }
        if(m_pointer == m_currentList.size()) {
            String continuationToken = m_currentList.get(m_pointer - 1);
            m_currentList = DBService.instance(m_tenant).getRows(m_storeName, continuationToken, m_chunkSize);
            m_pointer = 0;
            // if the first row in the next chunk equals to the next row
            if(m_currentList.size() > 0 && continuationToken.equals(m_currentList.get(0))) {
                m_pointer = 1;
            }
        }
        if(m_pointer >= m_currentList.size()) {
            return null;
        }
        return new DRow(m_tenant, m_storeName, m_currentList.get(m_pointer++));
    }
    
}
