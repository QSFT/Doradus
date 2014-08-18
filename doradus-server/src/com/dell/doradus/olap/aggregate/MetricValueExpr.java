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

import java.text.DecimalFormat;

import com.dell.doradus.search.aggregate.BinaryExpression.MetricOperation;

public abstract class MetricValueExpr implements IMetricValue {
	
	public static double getValue(IMetricValue value) {
		if(value instanceof MetricValueExpr) {
			return ((MetricValueExpr)value).getValue();
		}
		String str = value.toString();
		if(str == null || str.length() == 0) return Double.MIN_VALUE;
		else return Double.parseDouble(str);
	}
	
	public abstract double getValue();
	
	@Override public boolean isDegenerate() {
		double x = getValue();
		return x == Double.NEGATIVE_INFINITY || x == Double.POSITIVE_INFINITY || Double.isNaN(x);
	} 
	
	@Override public int compareTo(IMetricValue o) {
		return Double.compare(getValue(), getValue(o));
	}
	
	@Override public String toString() {
		double val = getValue();
		if(val == Double.MIN_VALUE || val == Double.MAX_VALUE) return null;
		if(val == Double.NEGATIVE_INFINITY || val == Double.POSITIVE_INFINITY) return null;
		long lval = Math.round(val);
		if(Math.abs(val - lval) < 0.001) return "" + lval;
		else return new DecimalFormat("#.#########").format(getValue());
		//else return String.format("%.3f", getValue());
		//else return Double.toString(val);
	}
	
	
	public static class Binary extends MetricValueExpr {
		public MetricOperation operator;
		public IMetricValue first;
		public IMetricValue second;

		@Override public double getValue() {
			double x = getValue(first);
			double y = getValue(second);
			switch(operator) {
				case DIVIDE: return x / y;
				case MULTIPLAY: return x * y;
				case PLUS: return x + y;
				case MINUS: return x - y;
				default: throw new IllegalArgumentException("Invalid Metric Operation: " + operator.toString());
			}
			
		}
		
		@Override public void reset() {
			first.reset();
			second.reset();
		}

		@Override public void add(long value) {
			first.add(value);
			second.add(value);
		}

		@Override public void add(IMetricValue value) {
			Binary b = (Binary)value;
			first.add(b.first);
			second.add(b.second);
		}
	}
	
	public static class Constant extends MetricValueExpr {
		public double value;

		public Constant(double value) { this.value = value; }
		
		@Override public double getValue() { return value; }
		
		@Override public void reset() { }

		@Override public void add(long value) { }

		@Override public void add(IMetricValue value) { }
	}
	
}
