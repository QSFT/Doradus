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

package com.dell.doradus.olap.store;

import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.search.Result;

public class FieldSearcher {
	private int m_documents;
	private int m_fields;
	private boolean m_bSingleValued;
	
	private int[] m_docterms;
	private int[] m_positions;
	
	public FieldSearcher(VDirectory dir, String table, String field) {
		if(!dir.fileExists(table + "." + field + ".doc")) return;
		
		if(dir.fileExists(table + "." + field + ".pos")) {
			m_bSingleValued = false;
			VInputStream inp_pos = dir.open(table + "." + field + ".pos");
			VInputStream inp_doc = dir.open(table + "." + field + ".doc");
			m_documents = inp_pos.readVInt();
			m_positions = new int[m_documents + 1];
			int docsSize = inp_doc.readVInt();
			m_docterms = new int[docsSize];
			
			m_positions[0] = 0;
			for(int i = 0; i < m_documents; i++) {
				int sz = inp_pos.readVInt();
				m_positions[i + 1] = m_positions[i] + sz;
				int term = 0;
				for(int j = 0; j < sz; j++) {
					term += inp_doc.readVInt();
					if(m_fields < term + 1) m_fields = term + 1;
					m_docterms[m_positions[i] + j] = term;
				}
			}
		} else {
			m_bSingleValued = true;
			VInputStream inp_doc = dir.open(table + "." + field + ".doc");
			m_documents = inp_doc.readVInt();
			m_docterms = new int[m_documents];
			for(int i = 0; i < m_documents; i++) {
				int term = inp_doc.readVInt() - 1;
				m_docterms[i] = term;
				if(m_fields < term + 1) m_fields = term + 1;
			}
		}
	}
	
	public int size() {
		if(m_docterms == null) return 0;
		else return m_bSingleValued ? m_docterms.length : m_positions.length - 1;
	}
	public int fields() { return m_fields; }
	public boolean isSingleValued() { return m_bSingleValued; }
	
	public void fields(int doc, IntIterator iter) {
		if(m_docterms == null) {
			iter.setup(null, 0, 0);
		} else if(m_bSingleValued) {
			iter.setup(m_docterms, doc, m_docterms[doc] < 0 ? 0 : 1);
		} else {
			iter.setup(m_docterms, m_positions[doc], m_positions[doc + 1] - m_positions[doc]);
		}
	}
	
	public void fields(Result src, Result dst) {
		if(m_docterms == null) return;
		
		if(m_bSingleValued) {
			for(int i = 0; i < m_documents; i++) {
				if(!src.get(i)) continue;
				if(m_docterms[i] != -1) dst.set(m_docterms[i]);
			}
		} else {
			for(int i = 0; i < m_documents; i++) {
				if(!src.get(i)) continue;
				int st = m_positions[i];
				int fn = m_positions[i + 1];
				for(int j = st; j < fn; j++) {
					dst.set(m_docterms[j]);
				}
			}
		}
	}
	

	public void fillDocs(Result valuesSet, Result docsSet) {
		if(m_docterms == null) return;
		if(m_bSingleValued) {
			for(int i = 0; i < m_documents; i++) {
				int value = m_docterms[i];
				if(value != -1 && valuesSet.get(value)) docsSet.set(i);
			}
		} else {
			for(int i = 0; i < m_documents; i++) {
				int st = m_positions[i];
				int fn = m_positions[i + 1];
				for(int j = st; j < fn; j++) {
					int value = m_docterms[j];
					if(value != -1 && valuesSet.get(value)) {
						docsSet.set(i);
						break;
					}
				}
			}
		}
	}

	public void fillValues(Result docsSet, Result valuesSet) {
		if(m_docterms == null) return;
		if(m_bSingleValued) {
			for(int i = 0; i < m_documents; i++) {
				if(!docsSet.get(i)) continue;
				int value = m_docterms[i];
				if(value != -1) valuesSet.set(value);
			}
		} else {
			for(int i = 0; i < m_documents; i++) {
				if(!docsSet.get(i)) continue;
				int st = m_positions[i];
				int fn = m_positions[i + 1];
				for(int j = st; j < fn; j++) {
					int value = m_docterms[j];
					if(value != -1) valuesSet.set(value);
				}
			}
		}
	}
	
	
	public void fill(int term, Result r) {
		fill(term, term + 1, r);
	}
	
	public void fill(int min, int max, Result r) {
		if(m_docterms == null) return;
		
		if(m_bSingleValued) {
			for(int i = 0; i < m_documents; i++) {
				if(m_docterms[i] < min) continue; 
				if(m_docterms[i] >= max) continue; 
				if(m_docterms[i] != -1) r.set(i);
			}
		} else {
			for(int i = 0; i < m_documents; i++) {
				int st = m_positions[i];
				int fn = m_positions[i + 1];
				for(int j = st; j < fn; j++) {
					if(m_docterms[j] >= min && m_docterms[j] < max) r.set(i);
				}
			}
		}
	}

	public void fillCount(int min, int max, Result r) {
		if(m_docterms == null) {
			r.clear();
			if(min <= 0 && max > 0) r.not();
			return;
		}
		if(m_bSingleValued) {
			for(int i = 0; i < m_documents; i++) {
				int count = m_docterms[i] == -1 ? 0 : 1; 
				if(min <= count && max > count) r.set(i);
			}
		} else {
			for(int i = 0; i < m_documents; i++) {
				int count = m_positions[i + 1] - m_positions[i]; 
				if(min <= count && max > count) r.set(i);
			}
		}
	}

	public void fillCount(int min, int max, Result filter, Result r) {
		if(m_docterms == null) {
			r.clear();
			if(min <= 0 && max > 0) r.not();
			return;
		}
		IntIterator iter = new IntIterator();
		for(int doc = 0; doc < m_documents; doc++) {
			fields(doc, iter);
			int count = 0;
			for(int i = 0; i < iter.count(); i++) {
				int field = iter.get(i);
				if(filter.get(field)) count++;
			}
			if(min <= count && max > count) r.set(doc);
		}
	}
	
	public long cacheSize()
	{
		return 16L + (m_docterms == null ? 0 : m_docterms.length * 4) + (m_positions == null ? 0 : m_positions.length * 4);
	}
	
}
