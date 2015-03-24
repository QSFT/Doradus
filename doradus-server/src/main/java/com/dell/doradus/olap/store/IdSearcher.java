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
import com.dell.doradus.olap.io.VInputStream;

public class IdSearcher {
	private VInputStream m_stream_id;
	private BSTR[] m_idx_ids;
	private long[] m_idx_offsets;
	private int m_size = 0;
	private BSTR m_bstr = new BSTR();
	private int m_cur_doc = -1;
	
	public IdSearcher(IdSearcher searcher) {
		m_stream_id = searcher.m_stream_id == null ? null : new VInputStream(searcher.m_stream_id);
		m_idx_ids = searcher.m_idx_ids;
		m_idx_offsets = searcher.m_idx_offsets;
		m_size = searcher.m_size;
		m_bstr = new BSTR(searcher.m_bstr);
		m_cur_doc = searcher.m_cur_doc;
	}
	
	public IdSearcher(VDirectory dir, SegmentStats.Table tableStats) {
		if(tableStats == null) return;
		String table = tableStats.name;
		m_size = tableStats.documents;
		m_stream_id = dir.open(table + "._id");
		m_stream_id.seek(0); // to read first chunk in-mem 
		m_idx_ids = new BSTR[(m_size + IdWriter.SPAN - 1) / IdWriter.SPAN];
		m_idx_offsets = new long[m_idx_ids.length];
		VInputStream stream_idx = dir.open(table + ".idx");
		long last_offset = 0;
		for(int i = 0; i < m_idx_ids.length; i++) {
			stream_idx.read(m_bstr);
			m_idx_ids[i] = new BSTR(m_bstr);
			last_offset = m_idx_offsets[i] = last_offset + stream_idx.readVLong();
		}
		m_bstr.length = -1;
	}
	
	public int size() { return m_size; }
	
	public int curDoc() { return m_cur_doc; }
	public BSTR curId() { return m_bstr; }

	public BSTR getId(int doc) {
		if(doc == m_cur_doc) return m_bstr;
		if(m_cur_doc < doc && m_cur_doc / IdWriter.SPAN == doc / IdWriter.SPAN) {
			while(m_cur_doc < doc) {
				m_cur_doc++;
				m_stream_id.readVString(m_bstr);
			}
		} else {
			m_stream_id.seek(m_idx_offsets[doc / IdWriter.SPAN]);
			m_bstr.set(m_idx_ids[doc / IdWriter.SPAN]);
			for(int i = 0; i <= doc % IdWriter.SPAN; i++) {
				m_stream_id.readVString(m_bstr);
			}
			m_cur_doc = doc;
		}
		return m_bstr;
	}

	public void reset() {
		if(m_stream_id == null) return;
		m_stream_id.seek(0);
		m_bstr.length = -1;
		m_cur_doc = -1;
	}
	
	public boolean next() {
		if(m_cur_doc >= m_size) throw new RuntimeException("Read past the end of IDSearcher");
		m_cur_doc++;
		if(m_cur_doc >= m_size) return false;
		m_stream_id.readVString(m_bstr);
		return true;
	}
	
	public int find(BSTR id, boolean exact) {
		if(m_size == 0) return -1;
		int min = 0;
		int max = m_idx_ids.length;
		while(max - min > 1) {
			int mid = (max + min) / 2;
			int c = BSTR.compare(id,  m_idx_ids[mid]);
			if(c < 0) max = mid;
			else min = mid;
		}
		m_stream_id.seek(m_idx_offsets[min]);
		m_cur_doc = min * IdWriter.SPAN;
		m_bstr.set(m_idx_ids[min]);
		m_stream_id.readVString(m_bstr);
		while(!m_stream_id.end() && BSTR.compare(id, m_bstr) > 0) {
			m_cur_doc++;
			m_stream_id.readVString(m_bstr);
		}
		if(exact && !BSTR.isEqual(id, m_bstr)) return -1;
		else return m_cur_doc;
	}

	public int findNext(BSTR id) {
		if(m_size == 0) return -1;
		if(m_cur_doc == -1) return find(id, true);
		if(m_cur_doc >= m_size) return -1;
		
		int curIdx = m_cur_doc / IdWriter.SPAN;
		if(curIdx == m_idx_ids.length - 1 ||
				(BSTR.compare(id,  m_bstr) >= 0 && BSTR.compare(id,  m_idx_ids[curIdx + 1]) < 0)) {
			while(!m_stream_id.end() && BSTR.compare(id, m_bstr) > 0) {
				m_cur_doc++;
				m_stream_id.readVString(m_bstr);
			}
			if(!BSTR.isEqual(id, m_bstr)) return -1;
			else return m_cur_doc;
		}
		else return find(id, true);
	}
	
	public long cacheSize()
	{
		long size = 1024 + m_bstr.buffer.length;
		if(m_idx_ids != null) for(BSTR b : m_idx_ids) size += b.buffer.length;
		if(m_idx_offsets != null) size += 8 * m_idx_offsets.length;
		return size;
	}
	
}
