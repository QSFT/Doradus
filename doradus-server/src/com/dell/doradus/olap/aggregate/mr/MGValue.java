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

package com.dell.doradus.olap.aggregate.mr;

public class MGValue implements Comparable<MGValue> {
	public Long[] Values;
	
	public MGValue(int size) {
		Values = new Long[size];
	}
	
	public MGValue(MGValue other) {
		Values = new Long[other.Values.length];
		for(int i = 0; i < Values.length; i++) Values[i] = other.Values[i];
	}
	
	@Override public int compareTo(MGValue o) {
		int c = 0;
		for(int i = 0; i < Values.length; i++) {
			c = Values[i].compareTo(o.Values[i]);
			if(c != 0) return c;
		}
		return 0;
	}
	
	@Override public boolean equals(Object obj) {
		MGValue o = (MGValue)obj;
		for(int i = 0; i < Values.length; i++) {
			if(!Values[i].equals(o.Values[i])) return false;
		}
		return true;
	}
	
	@Override public int hashCode() {
		int hc = 0;
		for(int i = 0; i < Values.length; i++) {
			hc += Values[i].hashCode();
		}
		return hc;
	}
}
