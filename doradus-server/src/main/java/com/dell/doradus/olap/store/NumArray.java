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

import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.io.VOutputStream;

public class NumArray {
	private int m_size;
	private int m_bits;
	private BitVector m_bitArray;
	private byte[] m_byteArray;
	private short[] m_shortArray;
	private int[] m_intArray;
	private long[] m_longArray;
	

	public NumArray(int size, int bits) {
		m_size = size;
		m_bits = bits;
		switch(bits) {
			case 0 : break;
			case 1 : m_bitArray = new BitVector(size); break; 
			case 8 : m_byteArray = new byte[size]; break; 
			case 16 : m_shortArray = new short[size]; break; 
			case 32 : m_intArray = new int[size]; break; 
			case 64 : m_longArray = new long[size]; break; 
			default: throw new RuntimeException("Unknown bits: " + bits);
		}
	}
	
	
	public NumArray(long[] values) {
	    long min = 0;
	    long max = 0;
	    for(int i = 0; i < values.length; i++) {
	        if(min > values[i]) min = values[i];
	        if(max < values[i]) max = values[i];
	    }
	    
	    m_bits = 0;
        m_size = values.length;
        if(max <= 1 && min >= 0) { // BitVector
            m_bits = 1;
            m_bitArray = new BitVector(m_size);
            for(int i = 0; i < m_size; i++) {
                if(values[i] != 0) m_bitArray.set(i);
                else m_bitArray.clear(i);
            }
        }
        else if(max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) { // 1 byte
            m_bits = 8;
            m_byteArray = new byte[m_size]; 
            for(int i = 0; i < m_size; i++) {
                m_byteArray[i] = (byte)values[i];
            }
        }
        else if(max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) { // 2 bytes
            m_bits = 16;
            m_shortArray = new short[m_size]; 
            for(int i = 0; i < m_size; i++) {
                m_shortArray[i] = (short)values[i];
            }
        }
        else if(max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) { // 4 bytes
            m_bits = 32;
            m_intArray = new int[m_size]; 
            for(int i = 0; i < m_size; i++) {
                m_intArray[i] = (int)values[i];
            }
        }
        else { // 8 bytes
            m_bits = 64;
            m_longArray = values; 
        }
	}
	
	public int size() { return m_size; }
	
	public long get(int index) {
		switch(m_bits) {
			case 0 : return 0;
			case 1 : return m_bitArray.get(index) ? 1 : 0; 
			case 8 : return m_byteArray[index]; 
			case 16 : return m_shortArray[index]; 
			case 32 : return m_intArray[index]; 
			case 64 : return m_longArray[index]; 
			default: throw new RuntimeException("Unknown bits: " + m_bits);
		}
	}
	
	public void set(int index, long value) {
		switch(m_bits) {
			case 0 : break;
			case 1 : if(value != 0) m_bitArray.set(index); else m_bitArray.clear(index); break; 
			case 8 : m_byteArray[index] = (byte)value; break; 
			case 16 : m_shortArray[index] = (short)value; break; 
			case 32 : m_intArray[index] = (int)value; break; 
			case 64 : m_longArray[index] = value; break; 
			default: throw new RuntimeException("Unknown bits: " + m_bits);
		}
	}
	
	public void load(VInputStream input) {
		switch(m_bits) {
		case 0 : break;
		case 1 :
			input.read(m_bitArray.getBuffer(), 0, m_bitArray.getBuffer().length);  
			break; 
		case 8 :
		    input.read(m_byteArray, 0, m_size);
			//for(int i = 0; i < m_size; i++) m_byteArray[i] = (byte)input.readByte();
			break;
		case 16 :
			for(int i = 0; i < m_size; i++) m_shortArray[i] = input.readShort();
			break;
		case 32 :
			for(int i = 0; i < m_size; i++) m_intArray[i] = input.readInt();
			break;
		case 64 :
			for(int i = 0; i < m_size; i++) m_longArray[i] = input.readLong();
			break;
		default: throw new RuntimeException("Unknown bits: " + m_bits);
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
		long size = 16;
		if(m_bitArray != null) size += m_bitArray.getBuffer().length;
		if(m_byteArray != null) size += m_byteArray.length * 1;
		if(m_shortArray != null) size += m_shortArray.length * 2;
		if(m_intArray != null) size += m_intArray.length * 4;
		if(m_longArray != null) size += m_longArray.length * 8;
		return size;
	}
	
}
