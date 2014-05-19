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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Field or chain of links in the form:
 * x.y.z, where x,y,z are LinkPathItem
 * Examples:
 * 
 *  Name
 *  Person.Name
 *  Sender.WHERE(Address IS NULL).Person._ID
 *  Sender.Person.Manager^._ID
 */
public class LinkPath {
	public List<LinkPathItem> path = new ArrayList<>();
	
	public LinkPath() {}
	public LinkPath(Collection<LinkPathItem> items) {
		path.addAll(items);
	}
	public LinkPath(LinkPathItem... items) {
		for(LinkPathItem item: items) path.add(item);
	}
	
	public int size() { return path.size(); }
	public LinkPathItem lastItem() { return path.get(path.size() - 1); }
	public void add(LinkPathItem item) { path.add(item); }
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		for(LinkPathItem item: path) {
			if(sb.length() > 0) sb.append('.');
			sb.append(item.toString());
		}
		return sb.toString();
	};
}
