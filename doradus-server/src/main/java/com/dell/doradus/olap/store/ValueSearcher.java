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
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VInputStream;

public class ValueSearcher {
	private VInputStream m_stream_term;
	private VInputStream m_stream_orig;
	private BSTR[] m_idx_terms;
	private BSTR[] m_idx_origs;
	private long[] m_term_offsets;
	private long[] m_orig_offsets;
	private int m_size = 0;
	private BSTR m_term = new BSTR();
	private int m_cur_term = -1;
	private BSTR m_orig = new BSTR();
	private int m_cur_orig = -1;
	
	public ValueSearcher(VDirectory dir, SegmentStats.Table.TextField fieldStats) {
		if(fieldStats == null) return;
		String table = fieldStats.table();
		String field = fieldStats.name;
		if(!dir.fileExists(table + "." + field + ".term")) return;
		m_stream_term = dir.open(table + "." + field + ".term");
		m_stream_term.seek(0);  // to read first chunk in-mem
		m_stream_orig = dir.open(table + "." + field + ".orig");
		m_stream_orig.seek(0);  // to read first chunk in-mem
		m_size = fieldStats.valuesCount;
		m_idx_terms = new BSTR[(m_size + ValueWriter.SPAN - 1) / ValueWriter.SPAN];
		m_idx_origs = new BSTR[(m_size + ValueWriter.SPAN - 1) / ValueWriter.SPAN];
		m_term_offsets = new long[m_idx_terms.length];
		m_orig_offsets = new long[m_idx_origs.length];
		VInputStream stream_term_idx = dir.open(table + "." + field + ".term.idx");
		long last_term_offset = 0;
		for(int i = 0; i < m_idx_terms.length; i++) {
			stream_term_idx.read(m_term);
			m_idx_terms[i] = new BSTR(m_term);
			last_term_offset = m_term_offsets[i] = last_term_offset + stream_term_idx.readVLong();
		}
		VInputStream stream_orig_idx = dir.open(table + "." + field + ".orig.idx");
		long last_orig_offset = 0;
		for(int i = 0; i < m_idx_origs.length; i++) {
			stream_orig_idx.read(m_orig);
			m_idx_origs[i] = new BSTR(m_orig);
			last_orig_offset = m_orig_offsets[i] = last_orig_offset + stream_orig_idx.readVLong();
		}
		m_term.length = -1;
		m_orig.length = -1;
	}
	
	public ValueSearcher(ValueSearcher searcher) {
		m_stream_orig = searcher.m_stream_orig == null ? null : new VInputStream(searcher.m_stream_orig);
		m_stream_term = searcher.m_stream_term == null ? null : new VInputStream(searcher.m_stream_term);
		m_idx_terms = searcher.m_idx_terms;
		m_idx_origs = searcher.m_idx_origs;
		m_term_offsets = searcher.m_term_offsets;
		m_orig_offsets = searcher.m_orig_offsets;
		m_size = searcher.m_size;
		m_term = new BSTR(searcher.m_term);
		m_cur_term = searcher.m_cur_term;
		m_orig = new BSTR(searcher.m_orig);
		m_cur_orig = searcher.m_cur_orig;
	}
	
	public int size() { return m_size; }
	
	
	public BSTR getValue(int orig) {
		if(orig == m_cur_orig) return m_orig;
		if(m_stream_orig == null) throw new FileDeletedException("in ValueSearcher");
		if(m_cur_orig < orig && m_cur_orig / ValueWriter.SPAN == orig / ValueWriter.SPAN) {
			while(m_cur_orig < orig) {
				m_cur_orig++;
				m_stream_orig.readVString(m_orig);
			}
		} else {
			m_stream_orig.seek(m_orig_offsets[orig / ValueWriter.SPAN]);
			m_orig.set(m_idx_origs[orig / ValueWriter.SPAN]);
			for(int i = 0; i <= orig % ValueWriter.SPAN; i++) {
				m_stream_orig.readVString(m_orig);
			}
			m_cur_orig = orig;
		}
		return m_orig;
	}
	
	public int curTermNumber() { return m_cur_term; }
	public BSTR curTerm() { return m_term; }

	public void reset() {
		if(m_stream_term == null) return;
		m_stream_term.seek(0);
		m_stream_orig.seek(0);
		m_term.length = -1;
		m_cur_term = -1;
		m_orig.length = -1;
		m_cur_orig = -1;
	}
	
	public boolean next() {
		if(m_cur_term >= m_size) return false;
		m_cur_term++;
		if(m_cur_term == m_size) return false;
		m_stream_term.readVString(m_term);
		return true;
	}
	
	public int find(BSTR term, boolean exact) {
		if(m_size == 0) return -1;
		int min = 0;
		int max = m_idx_terms.length;
		while(max - min > 1) {
			int mid = (max + min) / 2;
			int c = BSTR.compare(term,  m_idx_terms[mid]);
			if(c < 0) max = mid;
			else min = mid;
		}
		m_stream_term.seek(m_term_offsets[min]);
		m_term.set(m_idx_terms[min]);
		m_cur_term = min * IdWriter.SPAN;
		m_stream_term.readVString(m_term);
		while(!m_stream_term.end() && BSTR.compare(term, m_term) > 0) {
			m_cur_term++;
			m_stream_term.readVString(m_term);
		}
		if(exact && !BSTR.isEqual(term, m_term)) return -1;
		else if(!exact && BSTR.compare(term, m_term) > 0) return m_cur_term + 1;
		else return m_cur_term;
	}
	
	public long cacheSize()
	{
		long size = 1024 + m_term.buffer.length + m_orig.buffer.length;
		if(m_idx_terms != null) for(BSTR b : m_idx_terms) size += b.buffer.length;
		if(m_idx_origs != null) for(BSTR b : m_idx_origs) size += b.buffer.length;
		if(m_term_offsets != null) size += 8 * m_term_offsets.length;
		if(m_orig_offsets != null) size += 8 * m_orig_offsets.length;
		return size;
	}
	
}
