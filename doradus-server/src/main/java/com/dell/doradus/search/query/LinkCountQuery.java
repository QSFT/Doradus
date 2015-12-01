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
public class LinkCountQuery implements Query {
	// the name of the link field
	public String link;
	// the count of the linked objects
	public int count;
	// filter
	public Query filter;
	
	// temporary placeholder for XLinkQuery. TODO: replace queries with corresponding XLinkQueries
	public Query xlink;
	
	public LinkCountQuery() { }
	
	public LinkCountQuery(String link, int count) {
		this.link = link;
		this.count = count;
	}

	public LinkCountQuery(String link, int count, Query filter) {
		this.link = link;
		this.count = count;
		this.filter = filter;
	}
	
	@Override
	public String toString() {
		if(filter == null) return String.format("COUNT(%s) = %s", link, count);
		else return String.format("COUNT(%s.WHERE(%s)) = %s", link, filter, count);
	}
	
}
