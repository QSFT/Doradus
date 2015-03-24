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
 * Query that filters a document based on whether
 * any of its linked documents satisfies the query specified 
 */
public class TransitiveLinkQuery implements Query {
	// defines on which subset of linked objects the nested query should be true
	public String quantifier;
	//defines the depth. 0 means unlimited depth
	public int depth;
	// the name of the link field
	public String link;
	// The nested query on the linked documents
	public Query innerQuery;
	// filter
	public Query filter;
	
	public TransitiveLinkQuery() { }
	
	public TransitiveLinkQuery(String quantifier, int depth, String link, Query innerQuery) {
		this.quantifier = quantifier;
		this.depth = depth;
		this.link = link;
		this.innerQuery = innerQuery;
	}
	
	public Query getInnerQuery() {
		if(filter == null) return innerQuery;
		AndQuery andQuery = new AndQuery();
		andQuery.subqueries.add(filter);
		andQuery.subqueries.add(innerQuery);
		return andQuery;
	}
	
	@Override
	public String toString() {
		String dep = depth == 0 ? "" : "" + depth;
		if(filter == null) return String.format("%s(%s^%s).WHERE(%s)", quantifier, link, dep, innerQuery.toString());
		else return String.format("%s(%s.WHERE(%s)^%s).WHERE(%s)", quantifier, link, filter, dep, innerQuery.toString());
		//return String.format("%s(%s^%s).WHERE(%s)", quantifier, link, depth == 0 ? "" : "" + depth, getInnerQuery().toString());
	}
	
}
