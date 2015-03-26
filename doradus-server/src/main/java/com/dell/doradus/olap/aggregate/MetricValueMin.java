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

import java.util.Date;

import com.dell.doradus.olap.XType;

public abstract class MetricValueMin implements IMetricValue {
	public long metric = Long.MAX_VALUE;

	@Override public int compareTo(IMetricValue o) {
		MetricValueMin m = (MetricValueMin)o;
		long other_metric = m.metric;
		if(metric > other_metric) return 1;
		else if(metric < other_metric) return -1;
		else return 0;
	}

	@Override public boolean isDegenerate() { return metric == Long.MAX_VALUE; } 
	
	@Override public void reset() { metric = Long.MAX_VALUE; }

	@Override public void add(IMetricValue value) {
		MetricValueMin m = (MetricValueMin)value;
		add(m.metric);
	}

	public static class MinNum extends MetricValueMin {
		@Override public void add(long value) {
			if(metric > value) metric = value;
		}
		
		@Override public String toString() {
			if(metric == Long.MAX_VALUE) return null;
			else return "" + metric;
		}
		
		@Override public IMetricValue newInstance() { return new MinNum(); }
		@Override public IMetricValue convert(MetricCollector collector) { return this; }
	}
	
	public static class MinDate extends MetricValueMin {
		@Override public void add(long value) {
			if(metric > value && value > 0) metric = value;
		}
		
		@Override public String toString() {
			if(metric == Long.MAX_VALUE) return null;
			else return XType.toString(new Date(metric));
		}

		@Override public IMetricValue newInstance() { return new MinDate(); }
		@Override public IMetricValue convert(MetricCollector collector) { return this; }
	}
	
	public static class MinBoolean extends MetricValueMin {
		@Override public void add(long value) {
			if(metric > value) metric = value;
		}
		
		@Override public String toString() {
			if(metric == Long.MAX_VALUE) return null;
			else return XType.toString(metric > 0);
		}

		@Override public IMetricValue newInstance() { return new MinBoolean(); }
		@Override public IMetricValue convert(MetricCollector collector) { return this; }
	}
	
}
