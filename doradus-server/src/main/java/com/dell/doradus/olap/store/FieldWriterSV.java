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

public class FieldWriterSV {
	private int[] m_docs;
	private int m_maxTerm = -1;
	private int m_docsCount;
	
	public FieldWriterSV(int docsCount) {
		m_docsCount = docsCount;
		m_docs = new int[docsCount];
		for(int i = 0; i < docsCount; i++) m_docs[i] = -1;
	}
	
	public int getDocsCount() { return m_docsCount; }
	public int getValuesCount() { return m_maxTerm + 1; }
	
	public void set(int doc, int term) {
		if(term < 0) return;
		if(m_maxTerm < term) m_maxTerm = term;
		m_docs[doc] = term;
	}
	
	public void close(VDirectory dir, String table, String field) {
		if(m_maxTerm == -1) return;
		VOutputStream out_freq = dir.create(table + "." + field + ".freq");
        out_freq.writeVInt(m_docsCount);
		VOutputStream out_fdoc = dir.create(table + "." + field + ".fdoc");
		CompressedNumWriter w_freq = new CompressedNumWriter(out_freq, 16 * 1024);
		CompressedNumWriter w_fdoc = new CompressedNumWriter(out_fdoc, 16 * 1024);
		for(int i = 0; i < m_docs.length; i++) {
			int doc = m_docs[i];
			if(doc == -1) {
				w_freq.add(0);
			} else {
				w_freq.add(1);
				w_fdoc.add(doc);
			}
		}
		w_freq.close();
		w_fdoc.close();
		out_freq.close();
		out_fdoc.close();
		
		/*
        if(m_maxTerm == -1) return;
        VOutputStream out_doc = dir.create(table + "." + field + ".doc");
        out_doc.writeVInt(m_docsCount);
        for(int i = 0; i < m_docs.length; i++) {
            out_doc.writeVInt(m_docs[i] + 1);
        }
        out_doc.close();
        */
	}

}
