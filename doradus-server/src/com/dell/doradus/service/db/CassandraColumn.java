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

import com.dell.doradus.common.Utils;

/**
 * Represents a Doradus {@link DColumn} extracted from a Cassandra ColumnFamily. 
 */
public class CassandraColumn implements DColumn {
    private final String m_name;
    private final byte[] m_value;

    public CassandraColumn(byte[] name, byte[] value) {
        m_name = Utils.toString(name);
        m_value = value;
    }   // constructor
    
    @Override
    public String getName() {
        return m_name;
    }   // getName

    @Override
    public byte[] getRawValue() {
        return m_value;
    }   // getRawValue

    @Override
    public String getValue() {
        return Utils.toString(m_value);
    }   // getValue

}   // class CassandraColumn
