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
import java.util.List;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.NumWriterMV;

@SuppressWarnings("unchecked")
public class Doc {
	
	public BSTR id;
	public int number;
	public boolean deleted;

	private Object[] values;
	
	public static Comparator<Doc> COMP_ID = new DocIdComparator();
	public static Comparator<Doc> COMP_NO = new DocNumberComparator();
	
	public Doc(BSTR id, int fieldsCount) {
		this.id = new BSTR(id);
		values = new Object[fieldsCount];
	}

	public void addNumField(int fieldIndex, long value) {
		Object item = values[fieldIndex];
		Long val = new Long(value);
		if(item == null) values[fieldIndex] = val;
		else if(item instanceof Long) {
			List<Long> lst = new ArrayList<Long>(2);
			lst.add((Long)item);
			lst.add(val);
			values[fieldIndex] = lst;
		} else ((List<Long>)item).add(val);
	}
	

	public void addTextField(int fieldIndex, Term value) {
		Object item = values[fieldIndex];
		if(item == null) values[fieldIndex] = value;
		else if(item instanceof Term) {
			List<Term> lst = new ArrayList<Term>(2);
			lst.add((Term)item);
			lst.add(value);
			values[fieldIndex] = lst;
		} else ((List<Term>)item).add(value);
	}

	public void addLinkField(int fieldIndex, Doc linkedDoc) {
		Object item = values[fieldIndex];
		if(item == null) values[fieldIndex] = linkedDoc;
		else if(item instanceof Doc) {
			List<Doc> lst = new ArrayList<Doc>(2);
			lst.add((Doc)item);
			lst.add(linkedDoc);
			values[fieldIndex] = lst;
		} else ((List<Doc>)item).add(linkedDoc);
	}

	public void flushNumField(int fieldIndex, NumWriterMV writer) {
		Object item = values[fieldIndex];
		if(item == null) return;
		if(item instanceof Long) {
			Long value = (Long)item;
			writer.add(number, value.longValue());
		} else {
			List<Long> list = (List<Long>)item;
			Collections.sort(list);
			for(Long v : list) {
				writer.add(number, v.longValue());
			}
		}
	}
	
	public void flushTextField(int fieldIndex, FieldWriter writer) {
		Object item = values[fieldIndex];
		if(item == null) return;
		if(item instanceof Term) {
			Term term = (Term)item;
			writer.add(number, term.number);
		} else {
			List<Term> list = (List<Term>)item;
			Collections.sort(list, Term.COMP_NO);
			for(Term t : list) {
				writer.add(number, t.number);
			}
		}
	}

	public void flushLinkField(int fieldIndex, FieldWriter writer) {
		Object item = values[fieldIndex];
		if(item == null) return;
		if(item instanceof Doc) {
			Doc doc = (Doc)item;
			writer.add(number, doc.number);
		} else {
			List<Doc> list = (List<Doc>)item;
			Collections.sort(list, Doc.COMP_NO);
			for(Doc d : list) {
				writer.add(number, d.number);
			}
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
