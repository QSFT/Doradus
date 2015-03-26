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

public class MetricValueSum implements IMetricValue {
	public long metric;

	@Override public int compareTo(IMetricValue o) {
		MetricValueSum m = (MetricValueSum)o;
		long other_metric = m.metric;
		if(metric > other_metric) return 1;
		else if(metric < other_metric) return -1;
		else return 0;
	}
	
	@Override public boolean isDegenerate() { return false; }
	
	@Override public void reset() { metric = 0; }

	@Override public void add(IMetricValue value) {
		MetricValueSum m = (MetricValueSum)value;
		add(m.metric);
	}

	@Override public void add(long value) { metric += value; }
	
	@Override public String toString() { return "" + metric; }

	@Override public IMetricValue newInstance() { return new MetricValueSum(); }
	@Override public IMetricValue convert(MetricCollector collector) { return this; }
}
