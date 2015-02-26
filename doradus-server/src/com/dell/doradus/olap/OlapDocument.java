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

package com.dell.doradus.olap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OlapDocument {
	public String table;
	public String id;
	public boolean deleted;
	public Map<String, List<String>> fields = new HashMap<String, List<String>>();
	
	public OlapDocument() {}
	
	public OlapDocument(String table, String id) {
		this.table = table;
		this.id = id;
	}
	
	public OlapDocument addField(String field, String value) {
		if(value == null) return this;
		List<String> flds = fields.get(field);
		if(flds == null) {
			flds = new ArrayList<String>(1);
			fields.put(field, flds);
		}
		flds.add(value);
		return this;
	}
	
	public OlapDocument setDeleted(boolean deleted) {
		this.deleted = deleted;
		return this;
	}
	
	public String getTable() { return table; }
	public String getId() { return id; }
}
