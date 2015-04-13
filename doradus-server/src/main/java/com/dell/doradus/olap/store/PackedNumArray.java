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
import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.io.VOutputStream;

public class PackedNumArray {
	private int m_size;
	private int m_bits;
	private long m_min = 0;
	private long m_gcd = 1;
    private long[] m_values;

	public PackedNumArray(long[] values) {
	    m_size = values.length;
	    m_min = findMin(values);
	    m_gcd = findGcd(values);
	    m_bits = findBits(values);
        int packedSize = (m_size * m_bits + 63) / 64;
        m_values = new long[packedSize];
        BitPacker.pack(values, m_values, m_size, m_bits);
	}
	
	public PackedNumArray(VInputStream input) {
        m_size = input.readVInt();
        m_bits = input.readByte();
        m_min = input.readVLong();
        m_gcd = input.readVLong();
        int packedSize = input.readVInt();
        m_values = new long[packedSize];
        for(int i = 0; i < packedSize; i++) {
            m_values[i] = input.readLong();
        }
	}
	
	public int size() { return m_size; }
	public int bits() { return m_bits; }
	
	public long get(int index) {
	    long v = BitPacker.get(m_values, 0, m_bits, index);
	    return v;
	}
	
	public static PackedNumArray loadOldFormat(VInputStream input) {
	    int size = input.readVInt();
	    int bits = input.readByte();
	    NumArray na = new NumArray(size, bits);
	    na.load(input);
	    long[] values = new long[size];
	    for(int i = 0; i < values.length; i++) {
	        values[i] = na.get(i);
	    }
	    return new PackedNumArray(values);
	}

	public void write(VOutputStream output) {
	    output.writeVInt(m_size);
        output.writeByte((byte)m_bits);
        output.writeVLong(m_min);
        output.writeVLong(m_gcd);
        output.writeVInt(m_values.length);
        for(int i = 0; i < m_values.length; i++) {
            output.writeLong(m_values[i]);
        }
	}
	
	public static byte writeArray(long[] values, long min, long max, VOutputStream stream) {
		byte bits = 0;
		int size = values.length;
		stream.writeVInt(size);
		if(max <= 1 && min >= 0) { // BitVector
			bits = 1;
			stream.writeByte(bits);
			BitVector bv = new BitVector(size);
			for(int i = 0; i < size; i++) {
				if(values[i] != 0) bv.set(i);
				else bv.clear(i);
			}
			stream.write(bv.getBuffer(), 0, bv.getBuffer().length);
		}
		else if(max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) { // 1 byte
			bits = 8;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeByte((byte)values[i]);
			}
		}
		else if(max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) { // 2 bytes
			bits = 16;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeShort((short)values[i]);
			}
		}
		else if(max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) { // 4 bytes
			bits = 32;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeInt((int)values[i]);
			}
		}
		else { // 8 bytes
			bits = 64;
			stream.writeByte(bits);
			for(int i = 0; i < size; i++) {
				stream.writeLong((long)values[i]);
			}
		}
		stream.close();
		return bits;
	}
	
	
	public long cacheSize()
	{
	    return 24 + m_values.length * 8;
	}

    private long findMin(long[] values) {
        if(values.length == 0) return 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for(int i = 0; i < values.length; i++) {
            if(min > values[i]) min = values[i];
            if(max < values[i]) max = values[i];
        }
        if(max - min < 0) return 0; // range too big
        for(int i = 0; i < values.length; i++) {
            values[i] -= min;
        }
        return min;
    }

    // find greatest common divisor
    private long findGcd(long[] values) {
        long gcd = 0;
        for(int i = 0; i < values.length; i++) {
            gcd = NumericUtils.gcd(values[i], gcd);
            if(gcd == 1) break;
        }
        if(gcd > 1) {
            for(int i = 0; i < values.length; i++) {
                values[i] /= gcd;
            }
        }
        return gcd;
    }
    

    private int findBits(long[] values) {
        long range = 0;
        for(int i = 0; i < values.length; i++) {
            if(values[i] < 0) return 64;
            if(range < values[i]) range = values[i];
        }
        return BitPacker.bits(range);
    }
    
}
