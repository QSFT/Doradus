/*
 * Copyright (C) 2015 Dell, Inc.
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

package com.dell.doradus.olap.collections;

import java.util.ArrayList;
import java.util.List;

public class BdLongMap<V> {
	private BdLongSet m_set;
	private List<V> m_values;
	
	public BdLongMap(int capacity) {
		m_set = new BdLongSet(capacity);
		m_values = new ArrayList<V>(capacity);
	}

	//public BdLongMap(BdLongSet set) {
	//	m_set = set;
	//	m_set.restoreAfterSort();
	//	m_values = new ArrayList<V>(set.size());
	//}
	
	public int size() { return m_set.size(); }
	public V getAt(int index) { return m_values.get(index); }
	public V get(long key) { 
		int index = m_set.indexOf(key);
		if(index < 0) return null;
		return m_values.get(index);
	}
	
	public void put(long key, V value) {
		int index = m_set.add(key);
		if(index == m_values.size()) m_values.add(value);
		else m_values.set(index, value);
	}
	
}
