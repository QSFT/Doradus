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

package com.dell.doradus.search.rawquery;

/**
 * range with inclusive bounds:
 * x: [10 TO 20]   => BinaryQuery(x, RangePredicate("10", true, "20", true))
 * range with exclusive bounds:
 * x: {10 TO 20}   => BinaryQuery(x, RangePredicate("10", false, "20", false))
 * range with mixed bounds:
 * x: [10 TO 20}   => BinaryQuery(x, RangePredicate("10", true, "20", false))
 * x: {10 TO 20]   => BinaryQuery(x, RangePredicate("10", false, "20", true))
 * range with one bound:
 * x < 20          => BinaryQuery(x, RangePredicate(null, false, "20", false))
 * x <= 20
 * x > 10
 * x >= 10
 */
public class RangePredicate implements RawPredicate {
	public String min;
	public boolean minInclusive;
	public String max;
	public boolean maxInclusive;
	
	public RangePredicate() {}
	public RangePredicate(String min, boolean minInclusive, String max, boolean maxInclusive) {
		this.min = min;
		this.minInclusive = minInclusive;
		this.max = max;
		this.maxInclusive = maxInclusive;
	}
	
	@Override public String toString() {
		if(min == null) return (maxInclusive ? "<=" : "<") + Encoder.encode(max); 
		if(max == null) return (minInclusive ? ">=" : ">") + Encoder.encode(min); 
		StringBuilder sb = new StringBuilder();
		sb.append(":");
		sb.append(minInclusive ? '[' : '{');
		sb.append(Encoder.encode(min));
		sb.append(" TO ");
		sb.append(Encoder.encode(max));
		sb.append(maxInclusive ? ']' : '}');
		return sb.toString();
	}
}

