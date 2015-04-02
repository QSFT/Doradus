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
import com.dell.doradus.olap.io.VInputStream;


/**
 * first byte in each chunk is state:
 * 		1 - bits packing
 * 		2 - bits packing with outliers
 * 		3 - vint
 *		4 - single value
 */

public class CompressedNumReader {
	private VInputStream m_input;
	private long[] m_array;
	private long[] m_packed;
	private long[] m_temp;
	private int m_position;
	private int m_chunkSize;
	private int m_count;
	
	public CompressedNumReader(VInputStream input) {
		m_input = input;
		m_chunkSize = m_input.readVInt();
		m_array = new long[m_chunkSize];
		m_packed = new long[m_chunkSize];
		m_temp = new long[m_chunkSize];
		m_count = 0;
	}
	
	public long get() {
		if(m_position == m_count) readChunk();
		return m_array[m_position++];
	}
	
	public boolean isEnd() {
		return m_position == m_count && m_input.end();
	}
	
	private void readChunk() {
		m_position = 0;
		int state = m_input.readByte();
		m_count = m_input.readVInt();
		
		// single value
		if(state == 3) {
			long value = m_input.readVLong();
			for(int i = 0; i < m_count; i++) {
				m_array[i] = value;
			}
			return;
		}

		long min = m_input.readVLong();
		long gcd = m_input.readVLong();
		boolean isAscending = 1 == m_input.readByte();
		boolean isRunLength = 1 == m_input.readByte();
		
		
		// vint encoding
		if(state == 2) {
			for(int i = 0; i < m_count; i++) {
				m_array[i] = m_input.readVLong();
			}
		}
		else if(state == 1) {
			m_count = readPacked(m_array);
		}
		else throw new RuntimeException("Invalid state: " + state);

		if(isRunLength) {
		    int src_pos = 0;
		    int dst_pos = 0;
		    while(src_pos < m_count) {
                long value = m_array[src_pos++];
                
		        if(value != 0) {
                    m_temp[dst_pos++] = value - 1;
                    continue;
		        }
		        
		        int repeats = (int)m_array[src_pos++];
                value = m_array[src_pos++];
                for(int j = 0; j < repeats; j++) {
                    m_temp[dst_pos++] = value;
                }
                continue;
		        
		    }
			System.arraycopy(m_temp, 0, m_array, 0, dst_pos);
			m_count = dst_pos;
		}
		
		if(isAscending) {
			for(int i = 1; i < m_count; i++) {
				m_array[i] += m_array[i - 1];
			}
		}
		
		if(gcd != 1) {
			for(int i = 0; i < m_count; i++) {
				m_array[i] *= gcd;
			}
		}
		
		if(min != 0) {
			for(int i = 0; i < m_count; i++) {
				m_array[i] += min;
			}
		}
		
	}

	private int readPacked(long[] array) {
		int count = m_input.readVInt();
		int bits = m_input.readByte();
		int packedSize = (count * bits + 63) / 64;
		for(int i = 0; i < packedSize; i++) {
		    m_packed[i] = m_input.readLong();
		}
		BitPacker.unpack(m_packed, array, count, bits);
		return count;
	}
}
