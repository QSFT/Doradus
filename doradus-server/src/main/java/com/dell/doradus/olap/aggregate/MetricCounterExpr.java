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

public abstract class MetricCounterExpr extends MetricCounter {
	
	public static class Binary extends MetricCounterExpr {
		public MetricCounter first;
		public MetricCounter second;

		@Override public void add(int doc, IMetricValue value) {
			MetricValueExpr.Binary b = (MetricValueExpr.Binary)value;
			first.add(doc, b.first);
			second.add(doc, b.second);
		}
		
	}
	
	public static class Constant extends MetricCounterExpr {

		@Override public void add(int doc, IMetricValue value) { }
		
	}
	
}
