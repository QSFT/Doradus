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

// stores numbers in a bit array using minimal number of bits.
public class BitArray {
	private long[] m_array;
	private int m_bits;
	private int m_size;
	
	public BitArray(int size, long maxValue) {
		m_size = size;
		while(maxValue != 0) {
			m_bits++;
			maxValue >>>= 1;
		}
		m_array = new long[(m_size * m_bits + 63) / 64];
	}

	public void set(int index, long value) {
		if(m_bits == 0) return;
		if(m_bits == 64) {
			m_array[index] = value;
			return;
		}
		int bit = index * m_bits;
		int byte1 = bit / 64;
		int off = bit - byte1 * 64;
		if(off + m_bits <= 64) {
			long mask = mask(m_bits) << off;
			m_array[byte1] = m_array[byte1] & ~mask | (value << off) & mask; 
		} else {
			long mask = mask(off);
			m_array[byte1] = m_array[byte1] & mask | (value << off);
			byte1++;
			off = off + m_bits - 64;
			mask = mask(off);
			m_array[byte1] = m_array[byte1] & ~mask | (value >>> (m_bits - off)) & mask; 
		}
	}
	
	public long get(int index) {
		if(m_bits == 0) return 0;
		if(m_bits == 64) {
			return m_array[index];
		}
		int bit = index * m_bits;
		int byte1 = bit / 64;
		int off = bit - byte1 * 64;
		if(off + m_bits <= 64) {
			long mask = mask(m_bits) << off;
			return (m_array[byte1] & mask) >>> off; 
		} else {
			long mask = mask(off);
			long part1 = (m_array[byte1] & ~mask) >>> off;
			byte1++;
			off = off + m_bits - 64;
			mask = mask(off);
			long part2 = (m_array[byte1] & mask) << (m_bits - off);
			return part1 + part2;
		}
		
	}
	
	public long[] getArray() { return m_array; } 
	
	private static long mask(int bits) { return (~(-(1L<<bits))); }
	
	public int size() { return m_size; }
	public int bits() { return m_bits; }
	public int storeSize() { return m_array.length; }
}
