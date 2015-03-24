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

package com.dell.doradus.olap.aggregate;


public class MetricValueSet implements Comparable<MetricValueSet> {
	public IMetricValue[] values;
	
	public MetricValueSet(int size) {
		values = new IMetricValue[size];
	}
	
	public void add(MetricValueSet other) {
		for(int i = 0; i < values.length; i++) {
			values[i].add(other.values[i]);
		}
	}
	
	public void reset() {
		for(int i = 0; i < values.length; i++) {
			values[i].reset();
		}
	}

	@Override public int compareTo(MetricValueSet o) {
		return values[0].compareTo(o.values[0]);
	}
	
	public boolean isDegenerate() { return values[0].isDegenerate(); }
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		if(values.length > 1) sb.append('[');
		for(IMetricValue v: values) sb.append(v.toString());
		if(values.length > 1) sb.append(']');
		return sb.toString();
	}
}
