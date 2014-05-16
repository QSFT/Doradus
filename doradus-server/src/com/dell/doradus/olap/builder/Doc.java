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

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.FieldWriterSV;

public class Doc {
	
	public BSTR id;
	public int number;
	public boolean deleted;

	public Map<String, Long> sv_numerics = new HashMap<String, Long>();
	public Map<String, Term> sv_fields = new HashMap<String, Term>();
	public Map<String, Doc> sv_links = new HashMap<String, Doc>();
	
	public Map<String, List<Long>> numerics = new HashMap<String, List<Long>>();
	public Map<String, List<Term>> fields = new HashMap<String, List<Term>>();
	public Map<String, List<Doc>> links = new HashMap<String, List<Doc>>();
	
	public static Comparator<Doc> COMP_ID = new DocIdComparator();
	public static Comparator<Doc> COMP_NO = new DocNumberComparator();
	
	public Doc(BSTR id) {
		this.id = new BSTR(id);
	}

	public void addNum(String field, long value) {
		List<Long> f = numerics.get(field);
		if(f == null) {
			f = new ArrayList<Long>(1);
			numerics.put(field, f);
		}
		f.add(value);
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

	
	public void addSVNum(String field, long value) {
		Utils.require(sv_numerics.get(field) == null, "Single-valued field cannot contain multiple values");
		sv_numerics.put(field, new Long(value));
	}
	
	public void addSVField(String field, Term value) {
		Utils.require(sv_fields.get(field) == null, "Single-valued field cannot contain multiple values");
		sv_fields.put(field, value);
	}
	
	public void addSVLink(String field, Doc linkedDoc) {
		Utils.require(sv_links.get(field) == null, "Single-valued field cannot contain multiple values");
		sv_links.put(field, linkedDoc);
	}
	
	public void flushSVField(String field, FieldWriterSV writer) {
		Term term = sv_fields.get(field);
		if(term == null) return;
		writer.set(number, term.number);
	}
	
	public void flushSVLink(String field, FieldWriterSV writer) {
		Doc doc = sv_links.get(field);
		if(doc == null) return;
		writer.set(number, doc.number);
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
