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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *  (x) OR (y) OR (z) 
 */
public class OrQuery implements RawQuery {
	public List<RawQuery> subqueries = new ArrayList<RawQuery>();

	public OrQuery() {}
	public OrQuery(Collection<?extends RawQuery> queries) {
		subqueries.addAll(queries);
	}
	public OrQuery(RawQuery... queries) {
		for(RawQuery query: queries) subqueries.add(query);
	}
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		for(RawQuery query: subqueries) {
			if(sb.length() > 0) sb.append("OR");
			sb.append('(');
			sb.append(query.toString());
			sb.append(')');
		}
		return sb.toString();
	}
}

