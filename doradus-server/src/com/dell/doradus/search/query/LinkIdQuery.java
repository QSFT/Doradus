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
 * any of its linked documents has the ID specified 
 */
public class LinkIdQuery implements Query {
	// defines on which subset of linked objects the query should be true
	public String quantifier;
	// the name of the link field
	public String link;
	// The ID of document that the link should point to
	public String id;
	
	// temporary placeholder for XLinkQuery. TODO: replace queries with corresponding XLinkQueries
	public Query xlink;
	
	public LinkIdQuery() { }
	
	public LinkIdQuery(String quantifier, String link, String id) {
		this.quantifier = quantifier;
		this.link = link;
		this.id = id;
	}
	
	@Override public String toString() {
		return String.format("%s(%s)=%s", quantifier, link, id);
	} 
}
