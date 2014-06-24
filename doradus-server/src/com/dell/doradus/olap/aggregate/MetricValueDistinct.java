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

import java.util.HashSet;
import java.util.Set;

public class MetricValueDistinct implements IMetricValue {
	private Set<Object> m_Values = new HashSet<Object>();

	public Set<Object> getValues() { return m_Values; }
	public long getValue() { return m_Values.size(); }
	
	@Override public int compareTo(IMetricValue o) {
		MetricValueDistinct other = (MetricValueDistinct)o;
		return Double.compare(getValue(), other.getValue());
	}

	@Override public String toString() { return "" + getValue(); }
	
	@Override public void reset() { m_Values.clear(); }

	@Override public void add(IMetricValue value) {
		MetricValueDistinct other = (MetricValueDistinct)value;
		m_Values.addAll(other.m_Values);
	}

	@Override public void add(long value) {
		m_Values.add(new Long(value));
	}
	
}
