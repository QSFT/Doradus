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

import com.dell.doradus.olap.collections.BitPacker;
import com.dell.doradus.olap.collections.NumericUtils;
import com.dell.doradus.olap.io.VOutputStream;

public class CompressedNumWriter {
	private VOutputStream m_output;
	private long[] m_array;
	private long[] m_packed;
	private int[] m_r;
	private int m_position;
	
	public CompressedNumWriter(VOutputStream output, int chunkSize) {
		m_output = output;
		m_array = new long[chunkSize];
		m_packed = new long[chunkSize];
		m_r = new int[65];
		m_output.writeVInt(chunkSize);
	}
	
	public void add(long value) {
		m_array[m_position++] = value;
		if(m_position == m_array.length) flush();
	}
	
	public void close() {
		flush();
	}

	public void flush() {
		int count = m_position;
		if(count == 0) return;

		//1. Offset by min value
		long minValue = Long.MAX_VALUE;
		long maxValue = Long.MIN_VALUE;
		
		for(int i = 0; i < count; i++) {
			long value = m_array[i];
			if(minValue > value) minValue = value;
			if(maxValue < value) maxValue = value;
		}
		long range = maxValue - minValue;
		if(range < 0) minValue = 0; // range is too big
		if(minValue != 0) {
			for(int i = 0; i < count; i++) {
				m_array[i] -= minValue;
			}
		}
		
		//2. Get GCD - Greatest Common Divisor or all values
		long gcd = 0;
		if(range > 0) { // otherwize all values are the same
			for(int i = 0; i < count; i++) {
				long value = m_array[i];
				gcd = NumericUtils.gcd(value, gcd);
				if(gcd == 1) break;
			}
		}
		else gcd = 1;
		
		if(gcd > 1) {
			for(int i = 0; i < count; i++) {
				m_array[i] /= gcd;
			}
			range /= gcd;
		}
		
		//3. check if ascending
		boolean isAscending = true;
		long prev = 0;
		for(int i = 0; i < count; i++) {
			long value = m_array[i];
			if(value < prev) {
				isAscending = false;
				break;
			}
			prev = value;
		}
		if(isAscending) {
			prev = 0;
			range = 0;
			for(int i = 0; i < count; i++) {
				long value = m_array[i];
				m_array[i] -= prev;
				prev = value;
				if(m_array[i] > range) range = m_array[i]; 
			}
		}

		int bits = NumericUtils.bits(range);
		
		//4. check if vint encoding is more efficient
		boolean vint = false;
		
		if(bits > 8) {
			int packed_bytes = (count * bits + 7) / 8;
			int bytes = 0;
			for(int i = 0; i < count; i++) {
				long value = m_array[i];
				do {
					bytes++;
					value >>= 7;
					if(bytes > packed_bytes) break;
				} while(value > 0);
			}
			if(bytes < packed_bytes) vint = true;
		}
		
		//5. check for outliers
		for(int i = 0; i < m_r.length; i++) m_r[i] = 0;
		for(int i = 0; i < count; i++) {
			long value = m_array[i];
			int c = 64 - Long.numberOfLeadingZeros(value);
			m_r[c]++;
		}
		for(int i = 1; i < m_r.length; i++) m_r[i] += m_r[i-1];
		
		int k = -1;
		int maxbits = bits * count;
		for(int i = 1; i < bits; i++) {
			int curbits = (i + 1) * count + (count - m_r[i]) * (bits - i);
			if(curbits < maxbits) {
				k = i;
				maxbits = curbits;
			}
		}
		System.out.println(""+k);
		
		m_output.writeByte((byte)1); // version
		m_output.writeVInt(count);
		m_output.writeVLong(minValue);
		m_output.writeVLong(gcd);
		m_output.writeByte(isAscending ? (byte)1 : (byte)0);
		
		if(vint) {
			m_output.writeByte((byte)65);
			for(int i = 0; i < count; i++) {
				m_output.writeVLong(m_array[i]);
			}
		}
		else {
			int packedSize = BitPacker.pack(m_array, m_packed, m_position, bits);
			m_output.writeByte((byte)bits);
			for(int i = 0; i < packedSize; i++) {
				m_output.writeLong(m_packed[i]);
			}
		}		
		m_position = 0;
	}

}
