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

import java.util.Comparator;

import com.dell.doradus.olap.io.BSTR;

public class Term {
	public BSTR term;
	public BSTR origTerm;
	public int number;
	public static Comparator<Term> COMP_ID = new TermIdComparator();
	public static Comparator<Term> COMP_NO = new TermNumberComparator();
	
	public Term(BSTR term, BSTR origTerm) {
		this.term = new BSTR(term);
		this.origTerm = new BSTR(origTerm);
	}
	
	public static class TermIdComparator implements Comparator<Term> {
		@Override public int compare(Term x, Term y) {
			return BSTR.compare(x.term, y.term);
		}
	}

	public static class TermNumberComparator implements Comparator<Term> {
		@Override public int compare(Term x, Term y) {
			return x.number - y.number;
		}
	}

}
