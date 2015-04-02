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
	private IntList m_doc;
	private int[] m_len;
	private boolean m_isSingleValued = true;
	private int m_lastDoc = -1;
	private int m_lastTerm = -1;
	private int m_maxTerm = -1;
	private int m_docsCount;
	
	public FieldWriter(int docsCount) {
		m_docsCount = docsCount;
		m_doc = new IntList(docsCount);
		m_len = new int[docsCount];
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
		if(m_lastDoc > doc) throw new RuntimeException("Invalid doc order");
		if(m_maxTerm < term) m_maxTerm = term;
		if(m_lastDoc == doc) {
			m_isSingleValued = false;
			if(m_lastTerm > term) throw new RuntimeException("Invalid term order");
		} else m_lastDoc = doc;
		m_doc.add(term);
		m_len[doc]++;
		m_lastTerm = term;
	}
	
	public void close(VDirectory dir, String table, String field) {
		VOutputStream out_freq = dir.create(table + "." + field + ".freq");
		out_freq.writeVInt(m_docsCount);
		VOutputStream out_fdoc = dir.create(table + "." + field + ".fdoc");
		CompressedNumWriter w_freq = new CompressedNumWriter(out_freq, 16 * 1024);
		CompressedNumWriter w_fdoc = new CompressedNumWriter(out_fdoc, 16 * 1024);

		VOutputStream out_ndoc = null;
		CompressedNumWriter w_ndoc = null;
		if(!m_isSingleValued) {
			out_ndoc = dir.create(table + "." + field + ".ndoc");
			w_ndoc = new CompressedNumWriter(out_ndoc, 16 * 1024);
		}

		int pos = 0;
		for(int i = 0; i < m_len.length; i++) {
			int len = m_len[i];
			w_freq.add(len);
			if(len > 0) {
				int doc = m_doc.get(pos++);
				w_fdoc.add(doc);
				for(int j = 1; j < len; j++) {
					int nextdoc = m_doc.get(pos++);
					w_ndoc.add(nextdoc - doc);
					doc = nextdoc;
				}
			}
		}
		if(pos != m_doc.size()) throw new RuntimeException("Inconsistency in FieldWriter");
		
		w_freq.close();
		w_fdoc.close();
		out_freq.close();
		out_fdoc.close();
		if(w_ndoc != null) {
			w_ndoc.close();
			out_ndoc.close();
		}
		
		/*
        if(m_isSingleValued) {
            VOutputStream out_doc = dir.create(table + "." + field + ".doc");
            out_doc.writeVInt(m_len.length);
            int start = 0;
            for(int i = 0; i < m_len.length; i++) {
                int len = m_len[i];
                if(len == 0) out_doc.writeVInt(0);
                else {
                    for(int j = 0; j < len; j++) {
                        out_doc.writeVInt(m_doc.get(start++) + 1);
                    }
                }
            }
            out_doc.close();
            if(start != m_doc.size()) throw new RuntimeException("FieldWriter: inconsistency in sv mode");
        }else {
            VOutputStream out_doc = dir.create(table + "." + field + ".doc");
            VOutputStream out_pos = dir.create(table + "." + field + ".pos");
            out_pos.writeVInt(m_len.length);
            out_doc.writeVInt(m_doc.size());
            int start = 0;
            for(int i = 0; i < m_len.length; i++) {
                int len = m_len[i];
                out_pos.writeVInt(len);
                int doc = 0;
                for(int  j = 0; j < len; j++) {
                    int next_doc = m_doc.get(start++);
                    out_doc.writeVInt(next_doc - doc);
                    doc = next_doc;
                }
            }
            out_doc.close();
            out_pos.close();
            if(start != m_doc.size()) throw new RuntimeException("FieldWriter: inconsistency in mv mode");
        }            
        */
	}

}
