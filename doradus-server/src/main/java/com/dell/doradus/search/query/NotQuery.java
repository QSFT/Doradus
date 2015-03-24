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
 * The query which is true iff the nested query is false
 */
public class NotQuery implements Query {
	// the negated query 
	public Query innerQuery;
	
	public NotQuery() { }
	
	public NotQuery(Query innerQuery) { this.innerQuery = innerQuery; }
	
	@Override
	public String toString() {
		return String.format("NOT (%s)", innerQuery);
	}
	
}
