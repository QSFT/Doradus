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
public class LinkQuery implements Query {
	//ANY quantifier
	public static String ANY = "Any";
	//ALL quantifier
	public static String ALL = "All";
	//NONE quantifier
	public static String NONE = "None";
	// filter
	public Query filter;
	
	// defines on which subset of linked objects the nested query should be true
	public String quantifier;
	// the name of the link field
	public String link;
	// The nested query on the linked documents
	public Query innerQuery;
	
	// temporary placeholder for XLinkQuery. TODO: replace queries with corresponding XLinkQueries
	public Query xlink;
	
	public LinkQuery() { }
	
	public LinkQuery(String quantifier, String link, Query innerQuery) {
		this.quantifier = quantifier;
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
		String innerStr =  innerQuery == null ? "null" : innerQuery.toString();
		if(filter == null) return String.format("%s(%s).WHERE(%s)", quantifier, link, innerStr);
		else return String.format("%s(%s.WHERE(%s)).WHERE(%s)", quantifier, link, filter, innerStr);
	}
	
}
