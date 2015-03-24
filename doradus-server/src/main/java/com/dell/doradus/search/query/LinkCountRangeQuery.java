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
 * Query that filters a document based on the number of objects
 * it is linked to 
 */
public class LinkCountRangeQuery implements Query {
	// the name of the link field
	public String link;
	// the range query
	public RangeQuery range;
	// filter
	public Query filter;
	
	// temporary placeholder for XLinkQuery. TODO: replace queries with corresponding XLinkQueries
	public Query xlink;
	
	public LinkCountRangeQuery() { }
	
	public LinkCountRangeQuery(String link, RangeQuery range) {
		this.link = link;
		this.range = range;
	}
	
	@Override
	public String toString() {
		if(filter == null) return String.format("COUNT(%s) IN %s", link, range.toString());
		else return String.format("COUNT(%s.WHERE(%s)) IN %s", link, filter, range.toString());
		
	}
	
}
