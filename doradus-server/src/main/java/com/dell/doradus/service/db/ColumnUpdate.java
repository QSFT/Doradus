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
 * Represents a column update, which is store name + row key + column name + column value
 */
public class ColumnUpdate implements Comparable<ColumnUpdate> {
    private String m_storeName;
    private String m_rowKey;
    private DColumn m_column;

    /**
     * Create a column update with the given store name, row key, column name and binary value
     */
    public ColumnUpdate(String storeName, String rowKey, String columnName, byte[] columnValue) {
        m_storeName = storeName;
        m_rowKey = rowKey;
        m_column = new DColumn(columnName, columnValue);
    }
    
    /**
     * Get the store name of the update
     */
    public String getStoreName() {
        return m_storeName;
    }

    /**
     * Get the row key of the update
     */
    public String getRowKey() {
        return m_rowKey;
    }

    /**
     * Get the column
     */
    public DColumn getColumn() {
        return m_column;
    }
    
    @Override
    public String toString() {
        return "+" + getStoreName() + "/" + getRowKey() + "/" + getColumn().toString();
    }

    @Override
    public int compareTo(ColumnUpdate o) {
        int c = 0;
        c = getStoreName().compareTo(o.getStoreName());
        if(c > 0) return c;
        c = getRowKey().compareTo(o.getRowKey());
        if(c > 0) return c;
        c = getColumn().compareTo(o.getColumn());
        return c;
    }

    @Override    
    public boolean equals(Object obj) {
        ColumnUpdate o = (ColumnUpdate)obj;
        return getStoreName().equals(o.getStoreName()) &&
               getRowKey().equals(o.getRowKey()) &&
               getColumn().equals(o.getColumn());
    }
    
    @Override
    public int hashCode() {
        return getStoreName().hashCode() +
               getRowKey().hashCode() +
               getColumn().hashCode();
    }
}
