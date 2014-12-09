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
 * Query that returns objects with the ID specified
 */
public class IdRangeQuery implements Query {
	public String min;
	public boolean minInclusive;
	public String max;
	public boolean maxInclusive;
	
	public IdRangeQuery() { }
	
	public IdRangeQuery(String min, boolean minInclusive, String max, boolean maxInclusive) {
		this.min = min;
		this.minInclusive = minInclusive;
		this.max = max;
		this.maxInclusive = maxInclusive;
	}

	@Override
	public String toString() {
		return String.format("_ID IN %s%s - %s%s", minInclusive ? "[" : "{", min, max, maxInclusive ? "]" : "}");
	}
	
}
