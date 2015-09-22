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


/**
 * Represents a full row delete (delete of all columns in the row), which is store name + row key
 */
public class RowDelete implements Comparable<RowDelete> {
    private String m_storeName;
    private String m_rowKey;

    /**
     * Create a column delete with the given store name, row key and column name
     */
    public RowDelete(String storeName, String rowKey) {
        m_storeName = storeName;
        m_rowKey = rowKey;
    }
    
    /**
     * Get the store name of the delete
     */
    public String getStoreName() {
        return m_storeName;
    }

    /**
     * Get the row key of the delete
     */
    public String getRowKey() {
        return m_rowKey;
    }
    
    
    @Override
    public String toString() {
        return "-" + getStoreName() + "/" + getRowKey();
    }

    @Override
    public int compareTo(RowDelete o) {
        int c = 0;
        c = getStoreName().compareTo(o.getStoreName());
        if(c > 0) return c;
        c = getRowKey().compareTo(o.getRowKey());
        return c;
    }

    @Override    
    public boolean equals(Object obj) {
        RowDelete o = (RowDelete)obj;
        return getStoreName().equals(o.getStoreName()) &&
               getRowKey().equals(o.getRowKey());
    }
    
    @Override
    public int hashCode() {
        return getStoreName().hashCode() +
               getRowKey().hashCode();
    }
}
