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

import java.util.LinkedHashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class LRUCache<K, T> extends LinkedHashMap<K, T>{
	private final int capacity;
	
	public LRUCache(int capacity) {
		super(capacity, 0.75f, true);
		this.capacity = capacity;
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry<K, T> eldest) {
	    return size() > capacity;
	}
	
	public int getCapacity() {
		return capacity;
	}
}
