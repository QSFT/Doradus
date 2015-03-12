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

public abstract class BdIntSet {
	private int m_capacity;
	private int m_size;
	private int m_mask;
	private int[] m_indexBuffer;
	
	public BdIntSet(int capacity) {
		m_size = 0;
		m_capacity = capacity;
		m_mask = capacity * 2 - 1;
		if((m_mask & (capacity * 2)) != 0) {
			capacity = Integer.highestOneBit(capacity) * 2;
			m_mask = capacity * 2 - 1;
			if((m_mask & (capacity * 2)) != 0) throw new RuntimeException("Capacity should be power of two");
		}
		m_indexBuffer = new int[capacity * 2];
		for(int i = 0; i < m_indexBuffer.length; i++) m_indexBuffer[i] = -1;
	}

	protected abstract int getHashCode(int x);
	protected abstract boolean isEqual(int x, int y);
	
	public int add(int value) {
		if(value != m_size) throw new RuntimeException("Cannot add values out of order");
		if(m_size == m_capacity) resize();
		int index = indexOf(value);
		if(index >= 0) return index;
		int pos = -index - 1;
		m_indexBuffer[pos] = value;
		return m_size++;
	}

	private int indexOf(int value) {
		int hash = getHashCode(value) & 0x1FFFFFFF;
		int pos = hash & m_mask;
		int index = m_indexBuffer[pos];
		if(index < 0) return - pos - 1;
		if(isEqual(index, value)) return index;
		int inc = ((hash >> 8) + hash) | 1;
		while(true) {
			hash += inc;
			pos = hash & m_mask;
			index = m_indexBuffer[pos];
			if(index < 0) return - pos - 1;
			if(isEqual(index, value)) return index;
		}
	}
	
	private int findSlot(int value) {
		int hash = getHashCode(value) & 0x1FFFFFFF;
		int pos = hash & m_mask;
		int index = m_indexBuffer[pos];
		if(index < 0) return pos;
		int inc = ((hash >> 8) + hash) | 1;
		do {
			hash += inc;
			pos = hash & m_mask;
			index = m_indexBuffer[pos];
		}
		while(index >= 0);
		return pos;
	}
	
	private void resize() {
		m_capacity *= 2;
		m_mask = m_capacity * 2 - 1;
		int[] indexBuffer = m_indexBuffer;
		m_indexBuffer = new int[m_capacity * 2];
		for(int i = 0; i < m_indexBuffer.length; i++) m_indexBuffer[i] = -1;
		for(int i = 0; i < indexBuffer.length; i++) {
			int value = indexBuffer[i];
			if(value < 0) continue;
			int pos = findSlot(value);
			m_indexBuffer[pos] = i;
		}
	}
}
