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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.FieldWriter;

public class Doc {
	
	public BSTR id;
	public int number;
	public boolean deleted;
	
	public Map<String, Long> numerics = new HashMap<String, Long>(1);
	public Map<String, List<Term>> fields = new HashMap<String, List<Term>>(1);
	public Map<String, List<Doc>> links = new HashMap<String, List<Doc>>(1);
	
	public static Comparator<Doc> COMP_ID = new DocIdComparator();
	public static Comparator<Doc> COMP_NO = new DocNumberComparator();
	
	public Doc(BSTR id) {
		this.id = new BSTR(id);
	}
	
	public void setLong(String field, long value) {
		numerics.put(field, new Long(value));
	}
	
	public void addField(String field, Term value) {
		List<Term> f = fields.get(field);
		if(f == null) {
			f = new ArrayList<Term>(1);
			fields.put(field, f);
		}
		f.add(value);
	}

	public void addLink(String link, Doc linkedDoc) {
		List<Doc> d = links.get(link);
		if(d == null) {
			d = new ArrayList<Doc>(1);
			links.put(link, d);
		}
		d.add(linkedDoc);
	}
	
	public void flushField(String field, FieldWriter writer) {
		List<Term> list = fields.get(field);
		if(list == null) return;
		Collections.sort(list, Term.COMP_NO);
		for(Term t : list) {
			writer.add(number, t.number);
		}
	}

	public void flushLink(String link, FieldWriter writer) {
		List<Doc> list = links.get(link);
		if(list == null) return;
		Collections.sort(list, Doc.COMP_NO);
		for(Doc d : list) {
			writer.add(number, d.number);
		}
	}
	
	public static class DocIdComparator implements Comparator<Doc> {
		@Override public int compare(Doc x, Doc y) {
			return BSTR.compare(x.id, y.id);
		}
	}

	public static class DocNumberComparator implements Comparator<Doc> {
		@Override public int compare(Doc x, Doc y) {
			return x.number - y.number;
		}
	}

}
