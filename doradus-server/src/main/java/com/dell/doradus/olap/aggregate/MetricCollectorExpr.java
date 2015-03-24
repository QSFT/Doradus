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

import com.dell.doradus.search.aggregate.BinaryExpression.MetricOperation;

public abstract class MetricCollectorExpr implements IMetricCollector {
	
	
	public static class Binary extends MetricCollectorExpr {
		public MetricOperation operation;
		public IMetricCollector first;
		public IMetricCollector second;
		
		@Override public IMetricValue get() {
			MetricValueExpr.Binary b = new MetricValueExpr.Binary();
			b.operator = operation;
			b.first = first.get();
			b.second = second.get();
			return b;
		}

		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueExpr.Binary b = (MetricValueExpr.Binary)value;
			b.first = first.convert(b.first);
			b.second = second.convert(b.second);
			return b;
		}
		
	}

	public static class Constant extends MetricCollectorExpr {
		public double value;
		
		@Override public IMetricValue get() {
			MetricValueExpr.Constant c = new MetricValueExpr.Constant(value);
			return c;
		}

		@Override public IMetricValue convert(IMetricValue value) { return value; }
		
	}
	
}	
