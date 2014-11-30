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

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.search.util.LRUSizeCache;

public class StorageHelper {
    private LRUSizeCache<String, byte[]> m_chunkCache;
    private IO m_io;

    public StorageHelper(IO io) {
    	m_io = io;
		int cacheSize = ServerConfig.getInstance().olap_file_cache_size_mb;  
		if(cacheSize > 0) {
			m_chunkCache = new LRUSizeCache<String, byte[]>(cacheSize, cacheSize * 1024L * 1024L);
		}
    }

	public void writeFileChunk(String app, String key, String columnName, byte[] value, boolean useCache, boolean uncompressed) {
		if(!uncompressed) {
			value = Compressor.compress(value);
		}
		write(app, key, columnName, value);
		if(useCache && m_chunkCache != null) {
			String cacheKey = app + "/" + key + "/" + columnName;
			m_chunkCache.put(cacheKey, value, value.length + 2 * cacheKey.length() + 16);
		}
	}

	public byte[] readFileChunk(String app, String key, String columnName, boolean useCache, boolean uncompressed) {
		if(useCache && m_chunkCache != null) {
			byte[] cached = m_chunkCache.get(app + "/" + key + "/" + columnName);
			if(cached != null) {
				if(!uncompressed) {
					cached = Compressor.uncompress(cached);
				}
				return cached;
			}
		}
		byte[] value = getValue(app, key, columnName);
		if(value == null) throw new FileDeletedException();
		if(useCache && m_chunkCache != null) {
			String k = app + "/" + key + "/" + columnName;
			m_chunkCache.put(k, value, value.length + 2 * k.length() + 16);
		}
		if(!uncompressed) {
			value = Compressor.uncompress(value);
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
	
	public void delete(String app, String key) {
		delete(app, key, null);
	}

	public byte[] getValue(String app, String key, String column) { 
		return m_io.getValue(app, key, column);
	}
	public List<ColumnValue> get(String app, String key, String prefix) {
		return m_io.get(app, key, prefix);
	}
	public void createCF(String name) { m_io.createCF(name); } 
	public void deleteCF(String name) { m_io.deleteCF(name); }
	
	protected void write(String app, String key, List<ColumnValue> values) {
		m_io.write(app, key, values);
	}
	protected void delete(String columnFamily, String key, String columnName) {
		m_io.delete(columnFamily, key, columnName);
	}
	
}

