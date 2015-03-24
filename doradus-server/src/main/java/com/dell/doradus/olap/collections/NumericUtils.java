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
public class NumericUtils {
	
	public static int bits(long maxValue) {
		int bits = 0;
		while(maxValue != 0) {
			bits++;
			maxValue >>>= 1;
		}
		return bits;
	}
	
	public static long mask(int bits) {
		if(bits > 64 || bits < 0) throw new RuntimeException("Wrong number of bits: " + bits);
		return bits == 64 ? -1L : (1l << bits) - 1;
	}
	
	public static int nextPowerOfTwo(int x) {
		if((x & (x - 1)) != 0) x = Integer.highestOneBit(x) * 2;
		if((x & (x - 1)) != 0) throw new RuntimeException("Error in nextPowerOfTwo");
		return x;
	}
	
	// greatest common divisor
	public static long gcd(long x, long y) {
		if(x < 0) x = -x;
		if(y < 0) y = -y;
		if(x < y) {
			long temp = x;
			x = y;
			y = temp;
		}
		long gcd = 1;
		while(y > 1) {
			if((x & 1) == 0) {
				if((y & 1) == 0) {
					gcd <<= 1;
					x >>= 1;
					y >>= 1;
				} else {
					x >>= 1;
				}
			} else {
				if((y & 1) == 0) {
					y >>= 1;
				} else {
					x = (x - y) >> 1;
				}
			}
			if(x < y) {
				long temp = x;
				x = y;
				y = temp;
			}
		}
		return y == 1 ? gcd : gcd * x;
	}
	
	
	
}
