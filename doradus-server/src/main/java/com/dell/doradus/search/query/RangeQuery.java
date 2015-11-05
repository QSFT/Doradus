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

package com.dell.doradus.search.query;

/**
 * Query that filters a field's value within the specified range  
 */
public class RangeQuery implements Query {
	// The field whose value should be filtered
	public String field;
	// The lower bound of the range
	// null means that there is no lower bound
	public String min;
	// If true then the lower bound is inclusive
	public boolean minInclusive;
	// The upper bound of the range
	// null means that there is no upper bound
	public String max;
	// If true then the upper bound is inclusive
	public boolean maxInclusive;
	
	public RangeQuery() { }
	
	public RangeQuery(String field, String min, boolean minInclusive, String max, boolean maxInclusive) {
		this.field = field;
		this.min = min;
		this.minInclusive = minInclusive;
		this.max = max;
		this.maxInclusive = maxInclusive;
	}
	
	@Override
	public String toString() {
		return String.format("%s IN %s%s - %s%s",
				field, minInclusive ? "[" : "{", min, max, maxInclusive ? "]" : "}");
	}

	public String getStringRange() {
		return String.format("%s%s - %s%s",
				minInclusive ? "[" : "{", min, max, maxInclusive ? "]" : "}");
	}
	
}
