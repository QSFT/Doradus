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

public abstract class MetricCollectorFloat {
	
	public static class Sum extends MetricCollectorDouble.Sum {
		@Override public void add(int field, IMetricValue value) {
			MetricValueFloat.Sum v = (MetricValueFloat.Sum)value;
			metric[field] += v.metric;
		}
		@Override public IMetricValue get(int field) {
			MetricValueFloat.Sum v = new MetricValueFloat.Sum();
			if(field >= 0) v.metric = metric[field];
			return v;
		}
	}
	
	public static class Min extends MetricCollectorDouble.Min {
		@Override public void add(int field, IMetricValue value) {
			MetricValueFloat.Min v = (MetricValueFloat.Min)value;
			if(metric[field] > v.metric) metric[field] = v.metric;
		}
		@Override public IMetricValue get(int field) {
			MetricValueFloat.Min v = new MetricValueFloat.Min();
			if(field >= 0) v.metric = metric[field];
			return v;
		}
	}

	public static class Max extends MetricCollectorDouble.Max {
		@Override public void add(int field, IMetricValue value) {
			MetricValueFloat.Max v = (MetricValueFloat.Max)value;
			if(metric[field] < v.metric) metric[field] = v.metric;
		}
		@Override public IMetricValue get(int field) {
			MetricValueFloat.Max v = new MetricValueFloat.Max();
			if(field >= 0) v.metric = metric[field];
			return v;
		}
	}
	
	public static class Avg extends MetricCollectorDouble.Avg {
		@Override public void add(int field, IMetricValue value) {
			MetricValueFloat.Avg v = (MetricValueFloat.Avg)value;
			count[field] += v.count;
			metric[field] += v.metric;
		}
		@Override public IMetricValue get(int field) {
			MetricValueFloat.Avg v = new MetricValueFloat.Avg();
			if(field >= 0) {
				v.count = count[field];
				v.metric = metric[field];
			}
			return v;
		}
	}
	
}
