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

package com.dell.doradus.olap.collections.strings;

import com.dell.doradus.olap.collections.ArrayOperations;
import com.dell.doradus.olap.collections.IIntComparer;
import com.dell.doradus.olap.collections.QuickSorter;
import com.dell.doradus.olap.collections.UTF8;
import com.dell.doradus.olap.io.BSTR;

public class StringList implements IIntComparer {
	private char[] m_buffer;
	private int m_bufferPosition;
	private int[] m_lengths;
	private int m_lengthPosition;
	private UTF8 m_utf8;
	private BSTR m_temp;
	private BSTR m_temp_lowercase;
	private char[] m_temp_for_lowercase;
	
	public StringList() {
		m_buffer = new char[65536];
		m_lengths = new int[256];
		m_utf8 = new UTF8();
		m_temp = new BSTR(1024);
		m_temp_lowercase = new BSTR(1024);
		m_temp_for_lowercase = new char[1024];
	}
	
	public void clear() {
		m_bufferPosition = 0;
		m_lengthPosition = 0;
	}
	
	public int add(String value) {
		if(value == null) value = "";
		int index = m_lengthPosition / 2;
		int length = value.length();
		ensureCapacity(length);
		ArrayOperations.copy(value, m_buffer, m_bufferPosition);
		m_lengths[m_lengthPosition++] = length;
		m_lengths[m_lengthPosition++] = m_bufferPosition;
		m_bufferPosition += length;
		return index;
	}
	
	public void set(int index, String value) {
		int lengthPosition = index * 2;
		if(lengthPosition < 0 || lengthPosition > m_lengthPosition - 2) throw new RuntimeException("Invalid Index");
		if(value == null) value = "";
		int length = value.length();
		ensureCapacity(length);
		ArrayOperations.copy(value, m_buffer, m_bufferPosition);
		m_lengths[lengthPosition++] = length;
		m_lengths[lengthPosition++] = m_bufferPosition;
		m_bufferPosition += length;
	}
	
	public String get(int index) {
		index *= 2;
		int xlen = m_lengths[index];
		int xpos = m_lengths[index + 1];
		return new String(m_buffer, xpos, xlen);
	}

	public BSTR getBinary(int index) {
		index *= 2;
		int xlen = m_lengths[index];
		int xpos = m_lengths[index + 1];
		m_temp.assertLength(xlen * 4);
		m_temp.length = m_utf8.encode(m_buffer, xpos, xlen, m_temp.buffer, 0);
		return m_temp;
	}

	public BSTR getBinaryLowercase(int index) {
		index *= 2;
		int xlen = m_lengths[index];
		int xpos = m_lengths[index + 1];
		
		if(m_temp_for_lowercase == null || m_temp_for_lowercase.length < xlen) {
			m_temp_for_lowercase = new char[xlen * 2];
		}
		ArrayOperations.copy(m_buffer, xpos, m_temp_for_lowercase, 0, xlen);
		UTF8.toLower(m_temp_for_lowercase, 0, xlen);
		m_temp_lowercase.assertLength(xlen * 4);
		m_temp_lowercase.length = m_utf8.encode(m_temp_for_lowercase, 0, xlen, m_temp_lowercase.buffer, 0);
		
		return m_temp_lowercase;
	}
	
	public int size() { return m_lengthPosition / 2; }
	
	public int[] sort() {
		int[] array = new int[size()];
		for(int i = 0; i < array.length; i++) array[i] = i;
		sort(array, array.length);
		return array;
	}
	
	public void sort(int[] array, int length) {
		//HeapSorter.sort(this, array, 0, length);
		QuickSorter.sort(this, array, 0, length);
	}
	
	private void ensureCapacity(int length) {
		while(m_bufferPosition + length > m_buffer.length) {
			m_buffer = ArrayOperations.realloc(m_buffer, m_buffer.length * 2);
		}
		if(m_lengthPosition == m_lengths.length) {
			m_lengths = ArrayOperations.realloc(m_lengths, m_lengths.length * 2);
		}
	}

	@Override public boolean isEqual(int x, int y) {
		x *= 2;
		y *= 2;
		int xlen = m_lengths[x];
		int ylen = m_lengths[y];
		int xpos = m_lengths[x + 1];
		int ypos = m_lengths[y + 1];
		return ArrayOperations.isEqual(m_buffer, xpos, xlen, m_buffer, ypos, ylen);
	}

	@Override public int getHash(int x) {
		x *= 2;
		int xlen = m_lengths[x];
		int xpos = m_lengths[x + 1];
		return ArrayOperations.getHash(m_buffer, xpos, xlen);
	}

	@Override public int compare(int x, int y) {
		x *= 2;
		y *= 2;
		int xlen = m_lengths[x];
		int ylen = m_lengths[y];
		int xpos = m_lengths[x + 1];
		int ypos = m_lengths[y + 1];
		return ArrayOperations.compare(m_buffer, xpos, xlen, m_buffer, ypos, ylen);
	}

	@Override public int getHash(Object o) {
		if(o instanceof String) {
			String str = (String)o;
			char[] chars = str.toCharArray();
			return ArrayOperations.getHash(chars, 0, chars.length);
		}
		return o.hashCode();
	}
	
	@Override public boolean isEqual(Object o, int y) {
		y *= 2;
		int ylen = m_lengths[y];
		int ypos = m_lengths[y + 1];
		if(o instanceof String) {
			String str = (String)o;
			char[] chars = str.toCharArray();
			return ArrayOperations.isEqual(chars, 0, chars.length, m_buffer, ypos, ylen);
		}
		return o.equals(get(y));
	}

}
