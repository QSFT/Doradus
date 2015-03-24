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

import java.util.ArrayList;
import java.util.List;

/**
 * Query of the form _ID IN ('x', 'y', 'z')
 */
public class IdInQuery implements Query {
	// The list of IDs
	public List<String> ids;
	
	public IdInQuery() { }
	
	public IdInQuery(List<String> ids) {
		this.ids = ids;
	}
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<ids.size(); i++) {
			if(i > 0) sb.append(',');
			sb.append('\'');
			sb.append(ids.get(i));
			sb.append('\'');
		}
		return String.format("_ID IN (%s)", sb.toString());
	}
	
	public static IdInQuery tryCreate(OrQuery q) {
		List<String> ids = new ArrayList<String>();
		for(Query ch : q.subqueries) {
			if(!(ch instanceof IdQuery)) return null;
			ids.add(((IdQuery)ch).id);
		}
		return new IdInQuery(ids);
	}
}
