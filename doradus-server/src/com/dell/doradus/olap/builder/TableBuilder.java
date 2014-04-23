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

package com.dell.doradus.olap.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.olap.io.BSTR;

public class TableBuilder {
	public String table;
	
	public IdBuilder documents = new IdBuilder();
	public Set<String> numericFields = new HashSet<String>();
	public Set<String> linkFields = new HashSet<String>();
	public Map<String, FieldBuilder> fields = new HashMap<String, FieldBuilder>();
	
	private BSTR bstr = new BSTR();
	private BSTR orig = new BSTR();
	
	public TableBuilder(String table) {
		this.table = table;
	}

	public Doc addDoc(String id) {
		bstr.set(id);
		return documents.add(bstr);
	}
	
	public void addNum(Doc doc, String field, long value) {
		numericFields.add(field);
		doc.numerics.put(field, value);
	}

	public void addTerm(Doc doc, String field, String term) {
		FieldBuilder b = fields.get(field);
		if(b == null) {
			b = new FieldBuilder(field);
			fields.put(field, b);
		}
		orig.set(term);
		bstr.set(term.toLowerCase());
		Term t = b.add(bstr, orig);
		doc.addField(field, t);
	}

	public void addLink(Doc doc, String link, Doc linkedDoc) {
		linkFields.add(link);
		doc.addLink(link, linkedDoc);
	}

}
