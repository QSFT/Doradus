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

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VOutputStream;

public class ValueWriter {
	public static int SPAN = 1024;
	private VOutputStream m_stream_term;
	private VOutputStream m_stream_orig;
	private VOutputStream m_stream_term_idx;
	private VOutputStream m_stream_orig_idx;
	private int m_terms;
	private BSTR m_last = new BSTR();
	private BSTR m_orig = new BSTR();
	private long m_last_term_position = 0;
	private long m_last_orig_position = 0;
	
	public ValueWriter(VDirectory dir, String table, String field) {
		m_stream_term = dir.create(table + "." + field + ".term");
		m_stream_orig = dir.create(table + "." + field + ".orig");
		m_stream_term_idx = dir.create(table + "." + field + ".term.idx");
		m_stream_orig_idx = dir.create(table + "." + field + ".orig.idx");
		m_terms = 0;
		m_last.length = -1;
	}
	
	public int add(BSTR term, BSTR orig) {
		if(BSTR.isEqual(m_last, term)) return m_terms - 1;
		if(m_terms % SPAN == 0) {
			m_stream_term_idx.write(term);
			long new_term_position = m_stream_term.position();
			m_stream_term_idx.writeVLong(new_term_position - m_last_term_position);
			m_last_term_position = new_term_position;
			m_stream_orig_idx.write(orig);
			long new_orig_position = m_stream_orig.position();
			m_stream_orig_idx.writeVLong(new_orig_position - m_last_orig_position);
			m_last_orig_position = new_orig_position;
		}
		m_stream_term.writeVString(term);
		m_last.set(term);
		m_stream_orig.writeVString(orig);
		m_orig.set(orig);
		return m_terms++;
	}
	
	public int size() { return m_terms; }
	
	public void close() {
		m_stream_term.close();
		m_stream_orig.close();
		m_stream_term_idx.close();
		m_stream_orig_idx.close();
	}
	
}
