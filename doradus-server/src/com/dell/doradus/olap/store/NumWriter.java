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

import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VOutputStream;

public class NumWriter {
	private long[] m_values;
	private BitVector m_mask;
	
	public long min = Long.MAX_VALUE;
	public long max = Long.MIN_VALUE;
	public long min_pos = Long.MAX_VALUE;
	public byte bits = 0;
	
	public NumWriter(int docsCount) {
		m_values = new long[docsCount];
		m_mask = new BitVector(docsCount);
	}
	
	public void add(int doc, long number) {
		m_values[doc] = number;
		m_mask.set(doc);
		if(min > number) min = number;
		if(max < number) max = number;
		if(number > 0 && min_pos > number) min_pos = number;
	}
	
	public void close(VDirectory dir, String table, String field) {
		int setCount = m_mask.bitsSet();
		if(setCount == 0) return;
		else if(setCount != m_values.length) {
			VOutputStream mask_stream = dir.create(table + "." + field + ".num.mask");
			mask_stream.write(m_mask.getBuffer(), 0, m_mask.getBuffer().length);
			mask_stream.close();
		}
		if(min_pos > max) min_pos = 0;
		int size = m_values.length;
		if(size == 0) return;
		VOutputStream stream = dir.create(table + "." + field + ".dat");
		stream.writeVInt(size);
		if(max <= 1 && min >= 0) { // BitVector
			bits = 1;
			stream.writeByte(bits);
			BitVector bv = new BitVector(size);
			for(int i = 0; i < size; i++) {
				if(m_values[i] != 0) bv.set(i);
				else bv.clear(i);
			}
			stream.write(bv.getBuffer(), 0, bv.getBuffer().length);
		}
		else if(max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) { // 1 byte
			bits = 8;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeByte((byte)m_values[i]);
			}
		}
		else if(max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) { // 2 bytes
			bits = 16;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeShort((short)m_values[i]);
			}
		}
		else if(max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) { // 4 bytes
			bits = 32;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeInt((int)m_values[i]);
			}
		}
		else { // 8 bytes
			bits = 64;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeLong((long)m_values[i]);
			}
		}
		stream.close();
	}

}
