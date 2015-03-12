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

public class IntSet {
	private int m_capacity;
	private int m_size;
	private int m_mask;
	private int[] m_values;
	private int[] m_hashes;
	private int[] m_indexes;
	private IIntComparer m_comparer;
	
	public IntSet(IIntComparer comparer, int capacity) {
		m_comparer = comparer;
		m_size = 0;
		m_capacity = capacity;
		m_mask = capacity * 2 - 1;
		if((m_mask & (capacity * 2)) != 0) {
			capacity = Integer.highestOneBit(capacity) * 2;
			m_mask = capacity * 2 - 1;
			if((m_mask & (capacity * 2)) != 0) throw new RuntimeException("Capacity should be power of two");
		}
		m_values = new int[capacity];
		m_hashes = new int[capacity];
		reindex();
	}

	public int size() { return m_size; }
	public int get(int index) { return m_values[index]; }
	
	public int add(int value) {
		if(m_size == m_capacity) resize();
		int index = indexOf(value);
		if(index >= 0) {
			m_values[index] = value;
			return index;
		}
		int pos = -index - 1;
		m_indexes[pos] = m_size;
		m_values[m_size] = value;
		m_hashes[m_size] = m_comparer.getHash(value);
		return m_size++;
	}

	
	public int indexOf(Object value) {
		int hash = m_comparer.getHash(value);
		int pos = hash & m_mask;
		int index = m_indexes[pos];
		if(index < 0) return - pos - 1;
		if(hash == m_hashes[index] && m_comparer.isEqual(value, m_values[index])) return index;
		int inc = ((hash >> 8) + hash) | 1;
		while(true) {
			pos = (pos + inc) & m_mask;
			index = m_indexes[pos];
			if(index < 0) return - pos - 1;
			if(hash == m_hashes[index] && m_comparer.isEqual(value, m_values[index])) return index;
		}
	}
	
	public int indexOf(int value) {
		int hash = m_comparer.getHash(value);
		int pos = hash & m_mask;
		int index = m_indexes[pos];
		if(index < 0) return - pos - 1;
		if(hash == m_hashes[index] && m_comparer.isEqual(value, m_values[index])) return index;
		int inc = ((hash >> 8) + hash) | 1;
		while(true) {
			pos = (pos + inc) & m_mask;
			index = m_indexes[pos];
			if(index < 0) return - pos - 1;
			if(hash == m_hashes[index] && m_comparer.isEqual(value, m_values[index])) return index;
		}
	}
	
	private int findSlot(int i) {
		int hash = m_hashes[i];
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
		m_values = ArrayOperations.realloc(m_values, m_capacity);
		m_hashes = ArrayOperations.realloc(m_hashes, m_capacity);
		reindex();
	}
	
	private void reindex() {
		m_indexes = new int[m_capacity * 2];
		for(int i = 0; i < m_indexes.length; i++) m_indexes[i] = -1;
		for(int i = 0; i < m_size; i++) {
			int pos = findSlot(i);
			m_indexes[pos] = i;
		}
	}
}
