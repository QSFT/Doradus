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


/**
 * first byte in each chunk is state:
 * 		1 - bits packing
 * 		2 - vint
 *		3 - single value
 */

public class CompressedNumWriter {
	private VOutputStream m_output;
	private long[] m_array;
	private long[] m_packed;
	private long[] m_temp;
	private int m_position;
	
	public CompressedNumWriter(VOutputStream output, int chunkSize) {
		m_output = output;
		m_array = new long[chunkSize];
		m_packed = new long[chunkSize];
		m_temp = new long[chunkSize];
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
		m_position = 0;
		if(count == 0) return;

		//Offset by min value
		long minValue = Long.MAX_VALUE;
		long maxValue = Long.MIN_VALUE;
		
		for(int i = 0; i < count; i++) {
			long value = m_array[i];
			if(minValue > value) minValue = value;
			if(maxValue < value) maxValue = value;
		}
		
		long range = maxValue - minValue;
		if(range < 0) minValue = 0; // range is too big
		
		//check for single value
		if(range == 0) {
			m_output.writeByte((byte)3);
			m_output.writeVInt(count);
			m_output.writeVLong(minValue);
			return;
		}
		
		if(minValue != 0) {
			for(int i = 0; i < count; i++) {
				m_array[i] -= minValue;
			}
		}
		
		//Get GCD - Greatest Common Divisor or all values
		long gcd = 0;
		if(range > 0) {
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
		
		//check if ascending
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

		//check for run-length encoding
		boolean runlength = false;
		if(range > 0) {
			long newrange = range + 1;
			int src_pos = 0;
			int dst_pos = 0;
            int repeats = 1;
			while(src_pos < count) {
			    long value = m_array[src_pos];
			    repeats = 1;
			    while(src_pos + repeats < count && m_array[src_pos + repeats] == value) repeats++;
			    if(repeats >= 16) {
                    m_temp[dst_pos++] = 0;
                    m_temp[dst_pos++] = repeats;
                    m_temp[dst_pos++] = value;
                    if(newrange < repeats) newrange = repeats;
                    src_pos += repeats;
                    continue;
			    }
                m_temp[dst_pos++] = value + 1;
                src_pos++;
			}
			
			
            int est_count = (count * BitPacker.bits(range) + 63) / 64;
            int new_est_count = (count * BitPacker.bits(newrange) + 63) / 64;
			
            if(new_est_count < est_count) {
			    System.out.println("RUNLEN: " + count + "( " + range + ") => " + dst_pos + "( " + newrange + ")");
				runlength = true;
				System.arraycopy(m_temp, 0, m_array, 0, dst_pos);
				count = dst_pos;
				range = newrange;
				est_count = new_est_count;
			}
		}

		int bits = BitPacker.bits(range);

		//check if vint encoding is more efficient
		boolean vint = false;
		
		if(bits > 8) {
	        int packed_bytes = ((count * bits + 63) / 64) * 8;
			int bytes = 0;
			for(int i = 0; i < count; i++) {
				long value = m_array[i];
				do {
					bytes++;
					value >>= 7;
					if(bytes > packed_bytes) break;
				} while(value > 0);
			}
			if(bytes < packed_bytes) {
				vint = true;
			}
		}

		// 1 - bits packing; 3 - vint
		int state = vint ? 2 : 1;
		m_output.writeByte((byte)state);
		m_output.writeVInt(count);
		m_output.writeVLong(minValue);
		m_output.writeVLong(gcd);
		m_output.writeByte(isAscending ? (byte)1 : (byte)0);
		m_output.writeByte(runlength ? (byte)1 : (byte)0);
		
		if(vint) {
			for(int i = 0; i < count; i++) {
				m_output.writeVLong(m_array[i]);
			}
		} else {
			writePacked(m_array, count, bits);
		}		
	}

	
	private void writePacked(long[] array, int count, int bits) {
		m_output.writeVInt(count);
		m_output.writeByte((byte)bits);
		int packedSize = BitPacker.pack(array, m_packed, count, bits);
		for(int i = 0; i < packedSize; i++) {
		    m_output.writeLong(m_packed[i]);
		}
	}
}
