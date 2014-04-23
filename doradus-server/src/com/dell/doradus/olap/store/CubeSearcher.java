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

package com.dell.doradus.olap.store;

import com.dell.doradus.olap.FieldsCache;
import com.dell.doradus.olap.io.VDirectory;

public class CubeSearcher {
	private VDirectory m_directory;
	private SegmentStats m_stats;
	private FieldsCache m_fieldsCache;
	
	public CubeSearcher(VDirectory directory, FieldsCache fieldsCache) {
		m_directory = directory;
		m_stats = SegmentStats.load(directory);
		m_fieldsCache = fieldsCache;
	}

	public String getId() { return m_directory.getRow(); }
	
	public SegmentStats getStats() { return m_stats; }
	
	public int getDocs(String table) {
		SegmentStats.Table t = m_stats.getTable(table);
		return t == null ? 0 : t.documents;
	}
	
	public IdSearcher getIdSearcher(String table) {
		String key = getId() + "/id/" + table;
		synchronized(this) {
			IdSearcher s = (IdSearcher)m_fieldsCache.get(key);
			if(s == null) {
				s = new IdSearcher(m_directory, m_stats.getTable(table));
				m_fieldsCache.put(key, s, s.cacheSize() + 2 * key.length());
			}
			return new IdSearcher(s);
		}
	}
	
	public FieldSearcher getFieldSearcher(String table, String field) {
		String key = getId() + "/fld/" + table + "/" + field;
		synchronized(this) {
			FieldSearcher s = (FieldSearcher)m_fieldsCache.get(key);
			if(s == null) {
				s = new FieldSearcher(m_directory, table, field);
				m_fieldsCache.put(key, s, s.cacheSize() + 2 * key.length());
			}
			return s;
		}
	}

	public NumSearcher getNumSearcher(String table, String field) {
		String key = getId() + "/num/" + table + "/" + field;
		synchronized(this) {
			NumSearcher s = (NumSearcher)m_fieldsCache.get(key);
			if(s == null) {
				s = new NumSearcher(m_directory, table, field);
				m_fieldsCache.put(key, s, s.cacheSize() + 2 * key.length());
			}
			return s;
		}
	}

	public ValueSearcher getValueSearcher(String table, String field) {
		String key = getId() + "/val/" + table + "/" + field;
		synchronized(this) {
			ValueSearcher s = (ValueSearcher)m_fieldsCache.get(key);
			if(s == null) {
				s = new ValueSearcher(m_directory, m_stats.getTextField(table, field));
				m_fieldsCache.put(key, s, s.cacheSize() + 2 * key.length());
			}
			return new ValueSearcher(s);
		}
	}
	
}
