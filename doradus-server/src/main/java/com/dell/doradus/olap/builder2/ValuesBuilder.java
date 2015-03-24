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

package com.dell.doradus.olap.builder2;

import com.dell.doradus.olap.collections.strings.BstrList;
import com.dell.doradus.olap.collections.strings.BstrSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.ValueWriter;

public class ValuesBuilder {
	private BstrSet m_terms;
	private BstrList m_origs;
	private int[] m_val_to_term;
	
	public ValuesBuilder() {
		m_terms = new BstrSet();
		m_origs = new BstrList();
	}
	
	public int addTerm(BSTR term, BSTR orig) {
		int index = m_terms.add(term);
		if(index == m_origs.size()) m_origs.add(orig);
		return index;
	}

	public int size() {	return m_terms.size(); }
	
	public int term(int val) { return m_val_to_term[val]; } 
	
	public void flush(ValueWriter writer) {
		int[] remap = m_terms.sort();
		m_val_to_term = new int[remap.length];
		
		for(int i = 0; i < m_val_to_term.length; i++) {
			int val = remap[i];
			m_val_to_term[val] = writer.add(m_terms.get(val), m_origs.get(val));
		}
	}

}
