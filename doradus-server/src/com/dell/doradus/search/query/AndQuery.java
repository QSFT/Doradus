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

/**
 * Disjunctive query of all its subqueries.
 */
public class AndQuery implements Query {
	//The list of subqueries
	public ArrayList<Query> subqueries = new ArrayList<Query>();

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < subqueries.size(); i++) {
			if(i > 0) sb.append(" AND ");
			sb.append('(');
			sb.append(subqueries.get(i).toString());
			sb.append(')');
		}
		return sb.toString();
	}
	
}
