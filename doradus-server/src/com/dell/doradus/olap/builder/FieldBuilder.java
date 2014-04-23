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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.ValueWriter;

public class FieldBuilder {
	public String fieldName;
	public Map<BSTR, Term> terms = new HashMap<BSTR, Term>();
	
	public FieldBuilder(String field) {
		fieldName = field;
	}
	
	public Term add(BSTR term, BSTR origTerm) {
		Term t = terms.get(term);
		if(t == null) {
			t = new Term(term, origTerm);
			terms.put(t.term, t);
		}
		return t;
	}

	public void flush(ValueWriter writer) {
		Term[] list = new Term[terms.size()];
		int c = 0;
		for(Term t : terms.values()) list[c++] = t;
		Arrays.sort(list, Term.COMP_ID);
		for(int i = 0; i < list.length; i++) {
			Term t = list[i];
			t.number = i;
			writer.add(t.term, t.origTerm);
		}
	}
	
}
