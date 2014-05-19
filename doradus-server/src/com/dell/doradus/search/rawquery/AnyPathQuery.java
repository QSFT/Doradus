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
 * Query that should be true for at least one of all possible paths: 
 * ANY(path) = x
 * ANY(path).t=x
 * ANY(path).WHERE(t=x)
 * 
 */
public class AnyPathQuery implements RawQuery {
	public LinkPath path;
	public RawQuery query;
	
	public AnyPathQuery() {}
	public AnyPathQuery(LinkPath path, RawQuery query) {
		this.path = path;
		this.query = query;
	}
	
	@Override public String toString() {
		return "ANY(" + path.toString() + ").WHERE(" + query.toString() + ")";
	}
}
