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
public class BitPacker {
	
    public static int bits(long maxValue) { return NumericUtils.bits(maxValue); }
    
	public static int pack(long[] input, long[] output, int count, int bits) {
	    if(bits == 0) return 0;
		if(bits == 64) {
			for(int i = 0; i < count; i++) { output[i] = input[i]; }
			return count;
		}
		for(int i = 0; i < output.length; i++) { output[i] = 0; }
		int index = 0;
		int offset = 0;
		long mask = NumericUtils.mask(bits);
		for(int i = 0; i < count; i++) {
			long value = input[i] & mask;
			output[index] |= (value << offset);
			if(offset + bits <= 64) {
				offset += bits;
				if(offset == 64) {
					offset = 0;
					index++;
				}
			} else {
				index++;
				output[index] |= (value >>> (64 - offset));
				offset = offset + bits - 64;
			}
			
		}
		return (count * bits + 63) / 64;
	}

	public static void unpack(long[] input, long[] output, int count, int bits) {
		if(bits == 64) {
			for(int i = 0; i < count; i++) { output[i] = input[i]; }
			return;
		}
		int index = 0;
		int offset = 0;
		long mask = NumericUtils.mask(bits);
		for(int i = 0; i < count; i++) {
			long value = (input[index] >>> offset);
			if(offset + bits <= 64) {
				offset += bits;
				if(offset == 64) {
					offset = 0;
					index++;
				}
			}
			else {
				index++;
				value |= input[index] << (64 - offset);
				offset = offset + bits - 64;
			}
			output[i] = value & mask;
		}
	}
	
	public static void set(long value, long[] output, int start, int bits, int pos) {
		if(bits == 64) {
			output[start + pos] = value;
			return;
		}
		int index = start + (pos * bits) / 64;
		int offset = (pos * bits) % 64;
		long mask = NumericUtils.mask(bits);
		value &= mask;
		output[index] |= (value << offset);
		if(offset + bits > 64) {
			index++;
			output[index] |= (value >>> (64 - offset));
		}
	}
	

	public static long get(long[] input, int start, int bits, int pos) {
	    if(bits == 0) return 0;
		if(bits == 64) return input[start + pos];
		int index = start + (pos * bits) / 64;
		int offset = (pos * bits) % 64;
		long mask = NumericUtils.mask(bits);
		long value = (input[index] >>> offset);
		if(offset + bits > 64) {
			index++;
			value |= input[index] << (64 - offset);
		}
		return value & mask;
	}
}
