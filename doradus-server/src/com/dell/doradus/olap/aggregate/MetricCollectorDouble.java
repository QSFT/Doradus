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

public abstract class MetricCollectorDouble {
	
	public static class Sum implements IMetricCollector {
		public double[] metric;

		@Override public void setSize(int size) { metric = new double[size]; }
		@Override public int getSize() { return metric.length; }
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		@Override public boolean requiresConversion() { return false; }
		
		@Override public void add(int field, IMetricValue value) {
			MetricValueDouble.Sum v = (MetricValueDouble.Sum)value;
			metric[field] += v.metric;
		}

		@Override public IMetricValue get(int field) {
			MetricValueDouble.Sum v = new MetricValueDouble.Sum();
			if(field >= 0) v.metric = metric[field];
			return v;
		}
	}
	
	public static class Min implements IMetricCollector {
		public double[] metric;

		@Override public void setSize(int size) { 
			metric = new double[size];
			for(int i = 0; i < size; i++) metric[i] = Double.POSITIVE_INFINITY;
		}
		
		@Override public int getSize() { return metric.length; }
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		@Override public boolean requiresConversion() { return false; }
		
		@Override public void add(int field, IMetricValue value) {
			MetricValueDouble.Min v = (MetricValueDouble.Min)value;
			if(metric[field] > v.metric) metric[field] = v.metric;
		}

		@Override public IMetricValue get(int field) {
			MetricValueDouble.Min v = new MetricValueDouble.Min();
			if(field >= 0) v.metric = metric[field];
			return v;
		}
	}

	public static class Max implements IMetricCollector {
		public double[] metric;

		@Override public void setSize(int size) { 
			metric = new double[size];
			for(int i = 0; i < size; i++) metric[i] = Double.NEGATIVE_INFINITY;
		}
		
		@Override public int getSize() { return metric.length; }
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		@Override public boolean requiresConversion() { return false; }
		
		@Override public void add(int field, IMetricValue value) {
			MetricValueDouble.Max v = (MetricValueDouble.Max)value;
			if(metric[field] < v.metric) metric[field] = v.metric;
		}

		@Override public IMetricValue get(int field) {
			MetricValueDouble.Max v = new MetricValueDouble.Max();
			if(field >= 0) v.metric = metric[field];
			return v;
		}
	}
	
	public static class Avg implements IMetricCollector {
		public int[] count;
		public double[] metric;

		@Override public void setSize(int size) {
			count = new int[size];
			metric = new double[size];
		}
		
		@Override public int getSize() { return metric.length; }
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		@Override public boolean requiresConversion() { return false; }
		
		@Override public void add(int field, IMetricValue value) {
			MetricValueDouble.Avg v = (MetricValueDouble.Avg)value;
			count[field] += v.count;
			metric[field] += v.metric;
		}

		@Override public IMetricValue get(int field) {
			MetricValueDouble.Avg v = new MetricValueDouble.Avg();
			if(field >= 0) {
				v.count = count[field];
				v.metric = metric[field];
			}
			return v;
		}
	}
	
}
