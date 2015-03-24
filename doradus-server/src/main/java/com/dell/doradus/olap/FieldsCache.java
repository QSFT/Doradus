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

package com.dell.doradus.olap;

import com.dell.doradus.search.util.LRUSizeCache;

public class FieldsCache {
	private LRUSizeCache<String, Object> m_FieldsCache;
	
	public FieldsCache(long totalSizeInBytes) {
		m_FieldsCache = new LRUSizeCache<String, Object>(0, totalSizeInBytes);
	}
	
	public Object get(String key) {
		synchronized (m_FieldsCache) {
			return m_FieldsCache.get(key);
		}
	}

	public void put(String key, Object value, long size) {
		synchronized (m_FieldsCache) {
			m_FieldsCache.put(key, value, size);
		}
	}

	
}
