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

import java.util.Arrays;


public class LongList {
	private long[] m_array;
	private int m_position = 0;
	
	public LongList() { this(1024); }
	public LongList(int capacity) {
		m_array = new long[capacity];
	}
	
	public void add(long value) {
		if(m_position == m_array.length) resize();
		m_array[m_position++] = value;
	}
	
	public long get(int index) {
		if(index < 0 || index >= m_position) {
			throw new RuntimeException("LongList out of bounds: index=" + index + "; size=" + m_position);
		}
		return m_array[index];
	}
	
	public void set(int index, long value) { m_array[index] = value; }
	
	public int size() { return m_position; }

	private void resize() {
		long[] array = new long[m_array.length * 2];
		for(int i = 0; i < m_array.length; i++) array[i] = m_array[i];
		m_array = array;
	}
	
	public long[] getArray() { return m_array; }
	
	public void clear() { m_position = 0; }
	
	public void sort() {
		Arrays.sort(m_array, 0, m_position);
	}
	
	public void shrinkToSize() {
	    if(m_position == m_array.length) return;
		long[] array = new long[m_position];
		for(int i = 0; i < m_position; i++) array[i] = m_array[i];
		m_array = array;
	}
}
