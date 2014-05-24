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
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		
		@Override public IMetricValue get() {
			MetricValueDouble.Sum v = new MetricValueDouble.Sum();
			return v;
		}
	}
	
	public static class Min implements IMetricCollector {
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		
		@Override public IMetricValue get() {
			MetricValueDouble.Min v = new MetricValueDouble.Min();
			return v;
		}
	}

	public static class Max implements IMetricCollector {
		@Override public IMetricValue convert(IMetricValue value) { return value; }

		@Override public IMetricValue get() {
			MetricValueDouble.Max v = new MetricValueDouble.Max();
			return v;
		}
	}
	
	public static class Avg implements IMetricCollector {
		@Override public IMetricValue convert(IMetricValue value) { return value; }
		
		@Override public IMetricValue get() {
			MetricValueDouble.Avg v = new MetricValueDouble.Avg();
			return v;
		}
	}
	
}
