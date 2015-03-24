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

public class BitVector {
	private static final int[] COUNTS = new int[256]; 
	
	static {
		for(int b = 0; b < 256; b++) {
			int cnt = 0;
			if((b & 1) != 0) cnt++;
			if((b & 2) != 0) cnt++;
			if((b & 4) != 0) cnt++;
			if((b & 8) != 0) cnt++;
			if((b & 16) != 0) cnt++;
			if((b & 32) != 0) cnt++;
			if((b & 64) != 0) cnt++;
			if((b & 128) != 0) cnt++;
			COUNTS[b] = cnt;
		}
	}
	
	private int m_size;
	private byte[] m_buffer;
	private byte m_lastMask;
	
	
	public BitVector(int size) {
		m_size = size;
		m_buffer = new byte[(size + 7) / 8];
		m_lastMask = (byte)((1 << (size % 8)) - 1); 
	}

	public int size() { return m_size; }
	public byte[] getBuffer() { return m_buffer; }
	
	public void set(int i) {
		m_buffer[i / 8] |= (1 << (i % 8));
	}

	public void clear(int i) {
		m_buffer[i / 8] &= ~(1 << (i % 8));
	}
	
	public boolean get(int i) {
		return (m_buffer[i / 8] & (1 << (i % 8))) != 0;
	}
	
	public int bitsSet() {
		int cnt = 0;
		for(int i = 0; i < m_buffer.length; i++) {
			cnt += COUNTS[m_buffer[i] & 0xFF];
		}
		return cnt;
	}
	
	public void and(BitVector bv) {
		for(int i = 0; i < m_buffer.length; i++) m_buffer[i] &= bv.m_buffer[i];
	}
	public void or(BitVector bv) {
		for(int i = 0; i < m_buffer.length; i++) m_buffer[i] |= bv.m_buffer[i];
	}
	public void andNot(BitVector bv) {
		for(int i = 0; i < m_buffer.length; i++) m_buffer[i] &= ~bv.m_buffer[i];
	}
	public void not() {
		for(int i = 0; i < m_buffer.length; i++) m_buffer[i] = (byte)~m_buffer[i];
		if(m_lastMask != 0) m_buffer[m_buffer.length - 1] &= m_lastMask;
	}
	public void clearAll() {
		for(int i = 0; i < m_buffer.length; i++) m_buffer[i] = 0;
	}
	public void setAll() {
		for(int i = 0; i < m_buffer.length; i++) m_buffer[i] = -1;
	}
	
	public IntList getList() {
		IntList list = new IntList();
		for(int i = 0; i < m_buffer.length; i++) {
			int j = (i * 8);
			byte b = m_buffer[i];
			if(b == 0) continue;
			if((b & 1) != 0) list.add(j + 0);
			if((b & 2) != 0) list.add(j + 1);
			if((b & 4) != 0) list.add(j + 2);
			if((b & 8) != 0) list.add(j + 3);
			if((b & 16) != 0) list.add(j + 4);
			if((b & 32) != 0) list.add(j + 5);
			if((b & 64) != 0) list.add(j + 6);
			if((b & 128) != 0) list.add(j + 7);
		}
		list.shrinkToSize();
		return list;
	}
}
