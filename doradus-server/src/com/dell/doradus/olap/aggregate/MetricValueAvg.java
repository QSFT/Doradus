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

public abstract class MetricValueAvg implements IMetricValue {
	public int count;
	public long metric;

	public double getValue() { return count == 0 ? Double.NEGATIVE_INFINITY : (double)metric / count; }
	
	@Override public boolean isDegenerate() { return count == 0; } 
	
	@Override public int compareTo(IMetricValue o) {
		MetricValueAvg other = (MetricValueAvg)o;
		return Double.compare(getValue(), other.getValue());
	}

	@Override public void reset() { 
		count = 0;
		metric = 0;
	}

	@Override public void add(IMetricValue value) {
		MetricValueAvg other = (MetricValueAvg)value;
		count += other.count;
		metric += other.metric;
	}
	
	public static class AvgNum extends MetricValueAvg {
		@Override public void add(long value) {
			count++;
			metric += value;
		}
		
		@Override public String toString() {
			if(count == 0) return null;
			else return String.format("%.3f", getValue());
		}

	}

	public static class AvgDate extends MetricValueAvg {
		@Override public void add(long value) {
			if(value == 0) return;
			count++;
			metric += value;
		}
		
		@Override public String toString() {
			if(count == 0) return null;
			long val = metric / count / 1000 * 1000;
			return XType.toString(new Date(val));
		}

	}
	
}
