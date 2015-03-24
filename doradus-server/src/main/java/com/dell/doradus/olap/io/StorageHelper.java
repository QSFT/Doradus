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

package com.dell.doradus.olap.io;

import java.util.ArrayList;
import java.util.List;

public class StorageHelper {
	private FileCache m_fileCache;
    private IO m_io;

    public StorageHelper(IO io) {
    	m_io = io;
    	m_fileCache = new FileCache();
    }

	public void writeFileChunk(String app, String key, String columnName, byte[] value) {
		write(app, key, columnName, value);
		m_fileCache.put(app, key, columnName, value);
	}
	
	public void writeFileChunks(String app, String key, List<ColumnValue> columns) {
		write(app, key, columns);
		for(ColumnValue cv: columns) {
			m_fileCache.put(app, key, cv.columnName, cv.columnValue);
		}
	}
	
	public byte[] readFileChunk(String app, String key, String columnName) {
		byte[] value = m_fileCache.get(app, key, columnName);
		if(value == null) {
			value = getValue(app, key, columnName);
			if(value == null) throw new FileDeletedException();
			m_fileCache.put(app, key, columnName, value);
		}
		return value;
	}
	
	public void write(String app, String key, String columnName, byte[] value) {
		ColumnValue v = new ColumnValue(columnName);
		v.columnValue = value;
		List<ColumnValue> list = new ArrayList<ColumnValue>(1);
		list.add(v);
		write(app, key, list);
	}
	
	public void createCF(String name) { m_io.createCF(name); } 
	public void deleteCF(String name) { m_io.deleteCF(name); }
	
	public void delete(String app, String key) {
		delete(app, key, null);
	}
	public byte[] getValue(String app, String key, String column) { 
		return m_io.getValue(app, key, column);
	}
	public List<ColumnValue> get(String app, String key, String prefix) {
		return m_io.get(app, key, prefix);
	}
	protected void write(String app, String key, List<ColumnValue> values) {
		m_io.write(app, key, values);
	}
	protected void delete(String columnFamily, String key, String columnName) {
		m_io.delete(columnFamily, key, columnName);
	}
	
}

