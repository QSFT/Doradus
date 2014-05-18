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

public class GroupKey implements Comparable<GroupKey> {
	public long[] values;
	
	public GroupKey(int size) {
		values = new long[size];
	}
	
	public GroupKey(GroupKey other) {
		values = new long[other.values.length];
		for(int i = 0; i < values.length; i++) values[i] = other.values[i];
	}
	
	public void set(GroupKey other) {
		for(int i = 0; i < values.length; i++) values[i] = other.values[i];
	}
	
	@Override public int compareTo(GroupKey o) {
		for(int i = 0; i < values.length; i++) {
			if(values[i] < o.values[i]) return -1;
			if(values[i] > o.values[i]) return 1;
		}
		return 0;
	}
	
	@Override public boolean equals(Object obj) {
		GroupKey o = (GroupKey)obj;
		for(int i = 0; i < values.length; i++) {
			if(values[i] != o.values[i]) return false;
		}
		return true;
	}
	
	@Override public int hashCode() {
		int hc = 0;
		for(int i = 0; i < values.length; i++) {
			long val = values[i];
			hc *= 31;
			hc += (int)(val ^ (val >>> 32));
		}
		return hc;
	}
}
