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

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.search.util.LRUSizeCache;

public class FileCache {
    private LRUSizeCache<String, byte[]> m_chunkCache;

    public FileCache() {
		int cacheSize = ServerConfig.getInstance().olap_file_cache_size_mb;  
		if(cacheSize > 0) {
			m_chunkCache = new LRUSizeCache<String, byte[]>(0, cacheSize * 1024L * 1024L);
		}
    }

    public boolean useCache() { return m_chunkCache != null; }
    
	public void put(String app, String key, String columnName, byte[] value) {
		if(m_chunkCache == null) return;
		String cacheKey = app + "/" + key + "/" + columnName;
		m_chunkCache.put(cacheKey, value, value.length + 2 * cacheKey.length() + 16);
	}

	public byte[] get(String app, String key, String columnName) {
		if(m_chunkCache == null) return null;
		byte[] cached = m_chunkCache.get(app + "/" + key + "/" + columnName);
		return cached;
	}
	
}

