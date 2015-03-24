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
 * Item in the link path. Can contain WHERE filter, or be a transitive link:
 *  Name
 *  Person
 *  Sender.WHERE(Address IS NULL)
 *  Manager^10
 * 
 */
public class LinkPathItem {
	public String name;
	public RawQuery filter;
	public boolean isTransitive;
	public int transitiveDepth;
	
	public LinkPathItem() {}
	public LinkPathItem(String name) { this.name = name; }
	public LinkPathItem(String name, RawQuery filter) {
		this.name = name;
		this.filter = filter;
	}
	public LinkPathItem(String name, RawQuery filter, int transitiveDepth) {
		this.name = name;
		this.filter = filter;
		this.isTransitive = true;
		this.transitiveDepth = transitiveDepth;
	}
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		if(isTransitive) {
			sb.append('^');
			if(transitiveDepth != 0) sb.append(transitiveDepth);
		}
		if(filter != null) {
			sb.append(".WHERE(");
			sb.append(filter.toString());
			sb.append(')');
		}
		return sb.toString();
	};
}
