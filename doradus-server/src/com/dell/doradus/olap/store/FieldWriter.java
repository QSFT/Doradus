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
import com.dell.doradus.olap.io.VOutputStream;

public class FieldWriter {
	private IntList m_doc = new IntList(1024);
	private IntList m_pos;
	private boolean m_isSingleValued = true;
	private int m_lastDoc = -1;
	private int m_lastTerm = -1;
	private int m_maxTerm = -1;
	private int m_docsCount;
	
	public FieldWriter(int docsCount) {
		m_docsCount = docsCount;
		m_pos = new IntList(docsCount + 1);
	}
	
	public boolean isSingleValued() { return m_isSingleValued; }
	public int getDocsCount() { return m_docsCount; }
	public int getEntriesCount() { 
		if(m_isSingleValued) return getDocsCount();
		else return m_doc.size(); 
	}
	public int getValuesCount() { return m_maxTerm + 1; }
	
	public void add(int doc, int term) {
		if(m_lastDoc == doc && m_lastTerm == term) return;
		if(m_maxTerm < term) m_maxTerm = term;
		if(m_lastDoc == doc) {
			m_isSingleValued = false;
			if(m_lastTerm > term) throw new RuntimeException("Invalid term order");
		} else {
			while(m_lastDoc < doc) {
				m_pos.add(m_doc.size());
				m_lastDoc++;
			}
		}
		m_doc.add(term);
		m_lastTerm = term;
	}
	
	public void close(VDirectory dir, String table, String field) {
		while(m_pos.size() != m_docsCount + 1) m_pos.add(m_doc.size());
		
		if(m_isSingleValued) {
			VOutputStream out_doc = dir.create(table + "." + field + ".doc");
			out_doc.writeVInt(m_pos.size() - 1);
			for(int i = 1; i < m_pos.size(); i++) {
				int start = m_pos.get(i - 1);
				int end = m_pos.get(i);
				if(start == end) out_doc.writeVInt(0);
				else if(start + 1 != end) throw new RuntimeException("Invalid order");
				else out_doc.writeVInt(m_doc.get(start) + 1);
			}
			out_doc.close();
		}else {
			VOutputStream out_doc = dir.create(table + "." + field + ".doc");
			VOutputStream out_pos = dir.create(table + "." + field + ".pos");
			out_pos.writeVInt(m_pos.size() - 1);
			out_doc.writeVInt(m_doc.size());
			for(int i = 1; i < m_pos.size(); i++) {
				int start = m_pos.get(i - 1);
				int end = m_pos.get(i);
				out_pos.writeVInt(end - start);
				int doc = 0;
				for(int j = start; j < end; j++) {
					out_doc.writeVInt(m_doc.get(j) - doc);
					doc = m_doc.get(j);
				}
			}
			out_doc.close();
			out_pos.close();
		}
	}

}
