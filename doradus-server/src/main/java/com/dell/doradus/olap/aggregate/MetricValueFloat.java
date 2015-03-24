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

public abstract class MetricValueFloat {
	
	public static class Sum extends MetricValueDouble.Sum {
		@Override public void add(long value) { add(Float.intBitsToFloat((int)value)); }
	}
	
	public static class Min extends MetricValueDouble.Min {
		@Override public void add(long value) { add(Float.intBitsToFloat((int)value)); }
	}
	
	public static class Max extends MetricValueDouble.Max {
		@Override public void add(long value) { add(Float.intBitsToFloat((int)value)); }
	}
	
	public static class Avg extends MetricValueDouble.Avg {
		@Override public void add(long value) { add(Float.intBitsToFloat((int)value)); }
	}
	
}
