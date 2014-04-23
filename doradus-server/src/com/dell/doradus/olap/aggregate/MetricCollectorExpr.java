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
		
		@Override public IMetricValue get(int field) {
			MetricValueExpr.Binary b = new MetricValueExpr.Binary();
			b.operator = operation;
			b.first = first.get(field);
			b.second = second.get(field);
			return b;
		}

		@Override public void add(int field, IMetricValue value) {
			MetricValueExpr.Binary b = (MetricValueExpr.Binary)value;
			first.add(field, b.first);
			second.add(field, b.second);
		}

		@Override public void setSize(int size) {
			first.setSize(size);
			second.setSize(size);
		}

		@Override public int getSize() {
			return first.getSize();
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
		
		@Override public IMetricValue get(int field) {
			MetricValueExpr.Constant c = new MetricValueExpr.Constant();
			c.value = value;
			return c;
		}

		@Override public void add(int field, IMetricValue value) { }

		@Override public void setSize(int size) { }

		@Override public int getSize() { return 1; }

		@Override public IMetricValue convert(IMetricValue value) { return value; }
	}
	
}	
