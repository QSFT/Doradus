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

public class SimpleIntSet {
	private int m_capacity;
	private int m_size;
	private int m_mask;
	private int[] m_indexes;
	
	public SimpleIntSet(int capacity) {
		m_size = 0;
		m_capacity = capacity;
		m_mask = capacity * 2 - 1;
		if((m_mask & (capacity * 2)) != 0) {
			capacity = Integer.highestOneBit(capacity) * 2;
			m_mask = capacity * 2 - 1;
			if((m_mask & (capacity * 2)) != 0) throw new RuntimeException("Capacity should be power of two");
		}
		m_indexes = new int[m_capacity * 2];
		for(int i = 0; i < m_indexes.length; i++) m_indexes[i] = -1;
	}

	public int size() { return m_size; }

	public void clear() {
		m_size = 0;
		for(int i = 0; i < m_indexes.length; i++) m_indexes[i] = -1;
	}
	
	public int[] getBuffer() { return m_indexes; }
	
	public boolean add(int value) {
		if(value < 0) throw new RuntimeException("Cannot add negative numbers");
		if(m_size == m_capacity) resize();
		
		int hash = value;
		int pos = hash & m_mask;
		int index = m_indexes[pos];
		if(index == value) return false;
		if(index < 0) {
			m_indexes[pos] = value;
			m_size++;
			return true;
		}
		int inc = ((hash >> 8) + hash) | 1;
		while(true) {
			pos = (pos + inc) & m_mask;
			index = m_indexes[pos];
			if(index == value) return false;
			if(index < 0) {
				m_indexes[pos] = value;
				m_size++;
				return true;
			}
		}
	}

	
	private int findSlot(int i) {
		int hash = i;
		int pos = hash & m_mask;
		int index = m_indexes[pos];
		if(index < 0) return pos;
		int inc = ((hash >> 8) + hash) | 1;
		do {
			pos = (pos + inc) & m_mask;
			index = m_indexes[pos];
		}
		while(index >= 0);
		return pos;
	}
	
	
	private void resize() {
		m_capacity *= 2;
		m_mask = m_capacity * 2 - 1;
		reindex();
	}
	
	private void reindex() {
		int[] oldindexes = m_indexes;
		m_indexes = new int[m_capacity * 2];
		for(int i = 0; i < m_indexes.length; i++) m_indexes[i] = -1;
		for(int i = 0; i <oldindexes.length; i++) {
			int val = oldindexes[i];
			if(val == -1) continue;
			int pos = findSlot(val);
			m_indexes[pos] = val;
		}
	}
}
