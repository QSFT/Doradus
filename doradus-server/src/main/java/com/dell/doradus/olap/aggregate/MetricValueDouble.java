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

public abstract class MetricValueDouble {
	
	public static class Sum extends MetricValueExpr {
		public double metric;
		
		@Override public double getValue() { return metric; }
		@Override public void reset() { metric = 0; }
		@Override public void add(IMetricValue value) {
			Sum m = (Sum)value;
			add(m.metric);
		}
		public void add(double value) { metric += value; }
		@Override public void add(long value) { metric += Double.longBitsToDouble(value); }
	}
	
	
	public static class Min extends MetricValueExpr {
		public double metric = Double.POSITIVE_INFINITY;

		@Override public double getValue() { return metric; }
		@Override public void reset() { metric = Double.POSITIVE_INFINITY; }
		@Override public void add(IMetricValue value) {
			Min m = (Min)value;
			add(m.metric);
		}
		public void add(double value) { if(metric > value) metric = value; }
		@Override public void add(long value) { add(Double.longBitsToDouble(value)); }
	}

	public static class Max extends MetricValueExpr {
		public double metric = Double.NEGATIVE_INFINITY;
		
		@Override public double getValue() { return metric; }
		@Override public void reset() { metric = Double.NEGATIVE_INFINITY; }
		@Override public void add(IMetricValue value) {
			Max m = (Max)value;
			add(m.metric);
		}
		public void add(double value) { if(metric < value) metric = value; }
		@Override public void add(long value) { add(Double.longBitsToDouble(value)); }
	}
	
	public static class Avg extends MetricValueExpr {
		public int count;
		public double metric;

		public double getValue() { return count == 0 ? Double.NEGATIVE_INFINITY : (double)metric / count; }
		
		@Override public void reset() { 
			count = 0;
			metric = 0;
		}

		@Override public void add(IMetricValue value) {
			Avg other = (Avg)value;
			count += other.count;
			metric += other.metric;
		}
		
		public void add(double value) {
			count++;
			metric += value;
		}
		
		@Override public void add(long value) { add(Double.longBitsToDouble(value)); }
		
	}
	
}
