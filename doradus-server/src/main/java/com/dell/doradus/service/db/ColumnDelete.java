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
 * Represents a column delete, which is store name + row key + column name
 */
public class ColumnDelete implements Comparable<ColumnDelete> {
    private String m_storeName;
    private String m_rowKey;
    private String m_columnName;

    /**
     * Create a column delete with the given store name, row key and column name
     */
    public ColumnDelete(String storeName, String rowKey, String columnName) {
        m_storeName = storeName;
        m_rowKey = rowKey;
        m_columnName = columnName;
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
    
    /**
     * Get the column's name.
     */
    public String getColumnName() {
        return m_columnName;
    }
    
    @Override
    public String toString() {
        return "-" + getStoreName() + "/" + getRowKey() + "/" + getColumnName();
    }

    @Override
    public int compareTo(ColumnDelete o) {
        int c = 0;
        c = getStoreName().compareTo(o.getStoreName());
        if(c > 0) return c;
        c = getRowKey().compareTo(o.getRowKey());
        if(c > 0) return c;
        c = getColumnName().compareTo(o.getColumnName());
        return c;
    }

    @Override    
    public boolean equals(Object obj) {
        ColumnDelete o = (ColumnDelete)obj;
        return getStoreName().equals(o.getStoreName()) &&
               getRowKey().equals(o.getRowKey()) &&
               getColumnName().equals(o.getColumnName());
    }
    
    @Override
    public int hashCode() {
        return getStoreName().hashCode() +
               getRowKey().hashCode() +
               getColumnName().hashCode();
    }
}
