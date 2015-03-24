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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MemIO implements IO {
    private static Map<String, Map<String, byte[]>> m_map = new HashMap<String, Map<String, byte[]>>();
	
	public MemIO() { }
	
	@Override public byte[] getValue(String app, String key, String column) {
		synchronized(m_map) {
			Map<String, byte[]> m = m_map.get(app + "/" + key);
			if(m == null) return null;
			else return m.get(column);
		}
	}
	
	@Override public List<ColumnValue> get(String app, String key, String prefix) {
		List<ColumnValue> result = new ArrayList<ColumnValue>();
		synchronized(m_map) {
			Map<String, byte[]> m = m_map.get(app + "/" + key);
			if(m == null) return result;
			for(Map.Entry<String, byte[]> entry: m.entrySet()) {
				if(!entry.getKey().startsWith(prefix)) continue;
				result.add(new ColumnValue(entry.getKey().substring(prefix.length()), entry.getValue()));
			}
		}
			Collections.sort(result);
			return result;
	}
	
	@Override public void createCF(String name) {
	}

	@Override public void deleteCF(String name) {
		synchronized(m_map) {
			Set<String> toDelete = new HashSet<String>();
			for(String key: m_map.keySet()) {
				if(key.startsWith(name)) toDelete.add(key);
			}
			for(String key: toDelete) {
				m_map.remove(key);
			}
		}
	}
	
	@Override public void write(String app, String key, List<ColumnValue> values) {
		synchronized(m_map) {
			Map<String, byte[]> m = m_map.get(app + "/" + key);
			if(m == null) {
				m = new HashMap<String, byte[]>(1);
				m_map.put(app + "/" + key, m);
			}
			for(ColumnValue v: values) {
				m.put(v.columnName, v.columnValue);
			}
		}
	}
	
	@Override public void delete(String columnFamily, String sKey, String columnName) {
		synchronized(m_map) {
			Map<String, byte[]> m = m_map.get(columnFamily + "/" + sKey);
			if(m == null) return;
			if(columnName == null) {
				m_map.remove(columnFamily + "/" + sKey);
			} else {
				m.remove(columnName);
			}
		}
	}

}
