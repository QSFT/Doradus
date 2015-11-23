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

import java.util.Arrays;

public class BdLongSet {
	private long[] m_values;
	private int[] m_valueIndexes;
	private int m_size;
	private int m_mask;
	private int[] m_indexBuffer;
	private boolean m_bSorted;
	
	public BdLongSet(int capacity) {
		m_size = 0;
		capacity = NumericUtils.nextPowerOfTwo(capacity);
		m_mask = capacity * 2 - 1;
		m_values = new long[capacity];
		m_indexBuffer = new int[capacity * 2];
		for(int i = 0; i < m_indexBuffer.length; i++) m_indexBuffer[i] = -1;
	}
	
	public void enableClearBuffer() {
		if(m_size > 0) throw new RuntimeException("Cannot enable clearing on non-empty set");
		m_valueIndexes = new int[m_values.length];
	}
	public int size() { return m_size; }
	public long get(int index) { return m_values[index]; }
	
	public int indexOf(long value) {
		if(m_bSorted) throw new RuntimeException("Cannot add to sorted set");
		int hash = getHash(value);
		int pos = hash & m_mask;
		int index = m_indexBuffer[pos];
		if(index < 0) return - pos - 1;
		if(m_values[index] == value) return index;
		int inc = ((hash >> 8) + hash) | 1;
		while(true) {
			hash += inc;
			pos = hash & m_mask;
			index = m_indexBuffer[pos];
			if(index < 0) return - pos - 1;
			if(m_values[index] == value) return index;
		}
	}
	
	public int add(long value) {
		if(m_bSorted) throw new RuntimeException("Cannot add to sorted set");
		if(m_size == m_values.length) resize();
		int index = indexOf(value);
		if(index >= 0) return index;
		int pos = -index - 1;
		m_indexBuffer[pos] = m_size;
		m_values[m_size] = value;
		if(m_valueIndexes != null) m_valueIndexes[m_size] = pos;
		return m_size++;
	}
	
	public void addAll(BdLongSet values) {
		for(int i = 0; i < values.size(); i++) {
			add(values.get(i));
		}
	}
	
	public void sort() {
		Arrays.sort(m_values, 0, m_size);
		m_bSorted = true;
	}
	
	public void clear() {
		if(m_valueIndexes == null) throw new RuntimeException("Clearing is not enabled");
		while(m_size > 0) {
			m_size--;
			m_indexBuffer[m_valueIndexes[m_size]] = -1;
		}
		m_bSorted = false;
	}
	
	public void restoreAfterSort() {
		if(!m_bSorted) return;
		m_bSorted = false;
		for(int i = 0; i < m_indexBuffer.length; i++) m_indexBuffer[i] = -1;
		for(int i = 0; i < m_size; i++) {
			int pos = findSlot(m_values[i]);
			m_indexBuffer[pos] = i;
			if(m_valueIndexes != null) m_valueIndexes[i] = pos;
		}
	}
	
	public boolean intersects(BdLongSet other) {
	    if(size() == 0 && other.size() == 0) return true;
	    if(size() > other.size()) return other.intersects(this);
        for(int i = 0; i < size(); i++) {
            long value = get(i);
            if(other.indexOf(value) >= 0) return true;
        }
        return false;
	}

    public boolean equals(BdLongSet other) {
        if(size() != other.size()) return false;
        for(int i = 0; i < size(); i++) {
            long value = get(i);
            if(other.indexOf(value) < 0) return false;
        }
        return true;
    }

    public boolean contains(BdLongSet other) {
        if(other.size() > size()) return false;
        //special case: CONTAINS(X, null) should return false
        if(other.size() == 0 && size() != 0) return false;
        for(int i = 0; i < other.size(); i++) {
            long value = other.get(i);
            if(this.indexOf(value) < 0) return false;
        }
        return true;
    }

    public boolean disjoint(BdLongSet other) {
        return !intersects(other);
    }
    
    public boolean differs(BdLongSet other) {
    	return !equals(other);
    }
    
	private int findSlot(long value) {
		int hash = getHash(value);
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
		long[] values = m_values;
		int capacity = m_indexBuffer.length;
		m_mask = capacity * 2 - 1;
		m_values = new long[capacity];
		if(m_valueIndexes != null) m_valueIndexes = new int[capacity];
		m_indexBuffer = new int[capacity * 2];
		for(int i = 0; i < m_indexBuffer.length; i++) m_indexBuffer[i] = -1;
		for(int i = 0; i < m_size; i++) {
			m_values[i] = values[i];
			int pos = findSlot(values[i]);
			m_indexBuffer[pos] = i;
			if(m_valueIndexes != null) m_valueIndexes[i] = pos;
		}
	}
	
	private int getHash(long value) {
		return (int)(value ^ (value >>> 32));
	}
}
