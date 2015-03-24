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


public class IntList {
	private int[] m_array;
	private int m_position = 0;
	
	public IntList() { this(1024); }
	public IntList(int capacity) {
		m_array = new int[capacity];
	}
	
	public void add(int value) {
		if(m_position == m_array.length) resize();
		m_array[m_position++] = value;
	}
	
	public int get(int index) {
		if(index < 0 || index >= m_position) {
			throw new RuntimeException("IntList out of bounds: index=" + index + "; size=" + m_position);
		}
		return m_array[index];
	}
	
	public void set(int index, int value) { m_array[index] = value; }

	public int size() { return m_position; }
	public void setLength(int length) { m_position = length; }

	public void set(IntIterator iter) {
		iter.setup(m_array, 0, m_position);
	}
	
	public int[] getArray() { return m_array; }
	
	public void sort() {
		Arrays.sort(m_array, 0, m_position);
	}
	
	public void clear() { m_position = 0; }
	
	private void resize() {
		int[] array = new int[m_array.length * 2];
		for(int i = 0; i < m_array.length; i++) array[i] = m_array[i];
		m_array = array;
	}
	
	public void shrinkToSize() {
		int[] array = new int[m_position];
		for(int i = 0; i < m_position; i++) array[i] = m_array[i];
		m_array = array;
	}
}
