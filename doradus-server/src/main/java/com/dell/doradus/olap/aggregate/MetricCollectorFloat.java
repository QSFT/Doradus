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
		@Override public IMetricValue get() {
			MetricValueFloat.Sum v = new MetricValueFloat.Sum();
			return v;
		}
	}
	
	public static class Min extends MetricCollectorDouble.Min {
		@Override public IMetricValue get() {
			MetricValueFloat.Min v = new MetricValueFloat.Min();
			return v;
		}
	}

	public static class Max extends MetricCollectorDouble.Max {
		@Override public IMetricValue get() {
			MetricValueFloat.Max v = new MetricValueFloat.Max();
			return v;
		}
	}
	
	public static class Avg extends MetricCollectorDouble.Avg {
		@Override public IMetricValue get() {
			MetricValueFloat.Avg v = new MetricValueFloat.Avg();
			return v;
		}
	}
	
}
