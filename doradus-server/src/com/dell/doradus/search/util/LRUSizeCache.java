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

package com.dell.doradus.search.util;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LRUSizeCache<K, T> {
    private static Logger log = LoggerFactory.getLogger("Cache");
    
	private LinkedHashMap<K, ValueAndSize<T>> m_map;
	private final int m_capacity;
	private final long m_totalSize;
	private long m_currentSize; 
	
	public LRUSizeCache(int capacity, long totalSize) {
		m_map = new LinkedHashMap<K, ValueAndSize<T>>(capacity, 0.75f, true);
		m_capacity = capacity;
		m_totalSize = totalSize;
	}

	public T get(K key) {
		synchronized(this) {
			ValueAndSize<T> value = m_map.get(key);
			if(value == null) return null;
			else return value.value;
		}
	}
	
	public void put(K key, T value, long size) {
		synchronized(this) {
			ValueAndSize<T> oldValue = m_map.get(key);
			if(oldValue != null) {
				oldValue.value = value;
				m_currentSize -= oldValue.size;
				m_currentSize += size;
				log.warn("Updating existing data in the cache: might be an error: {}, {}->{}",
						new Object[] { key, oldValue.size, size});
			} else {
				m_currentSize += size;
				m_map.put(key, new ValueAndSize<T>(value, size));
			}
			if(m_currentSize > m_totalSize) {
				if(m_map.size() <= m_capacity) {
					log.warn("Cannot store capacity={} because the size limit is exceeded; currently: {}",
							m_capacity, m_map.size());
				}
				Iterator<Map.Entry<K, ValueAndSize<T>>> iterator = m_map.entrySet().iterator();
				while(iterator.hasNext()) {
					if(m_currentSize < m_totalSize) break;
					Map.Entry<K, ValueAndSize<T>> entry = iterator.next();
					iterator.remove();
					m_currentSize -= entry.getValue().size;
				}
			}
		}
	}
	
	public int size() {
		synchronized(this) {
			return m_map.size();
		}
	}

	public long storageSize () {
		synchronized(this) {
			return m_currentSize;
		}
	}
	
	public static class ValueAndSize<T> {
		public T value;
		public long size;
		
		public ValueAndSize(T value, long size) {
			this.value = value;
			this.size = size;
		}
	}
}
