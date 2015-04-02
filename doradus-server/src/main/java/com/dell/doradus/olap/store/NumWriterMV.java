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

public class NumWriterMV {
	private LongList m_doc = new LongList(1024);
	private int[] m_len;
	private boolean m_isSingleValued = true;
	
	public long min = Long.MAX_VALUE;
	public long max = Long.MIN_VALUE;
	public long min_pos = Long.MAX_VALUE;
	public byte bits = 0;
	
	private int m_lastDoc = -1;
	private long m_lastNum = -1;
	private long m_maxNum = -1;
	private int m_docsCount;
	
	public NumWriterMV(int docsCount) {
		m_docsCount = docsCount;
		m_len = new int[docsCount];
	}
	
	public int getDocListSize() { return m_doc.size(); }
	public boolean isSingleValued() { return m_isSingleValued; }
	public int getDocsCount() { return m_docsCount; }
	public int getEntriesCount() { 
		if(m_isSingleValued) return getDocsCount();
		else return m_doc.size(); 
	}
	
	public void add(int doc, long num) {
		if(m_lastDoc == doc && m_lastNum == num) return;
		if(m_lastDoc > doc) throw new RuntimeException("Invalid doc order");
		if(m_maxNum < num) m_maxNum = num;
		if(m_lastDoc == doc) {
			m_isSingleValued = false;
			if(m_lastNum > num) throw new RuntimeException("Invalid number order");
		} else m_lastDoc = doc; 
		m_doc.add(num);
		m_len[doc]++;
		m_lastNum = num;
		
		if(min > num) min = num;
		if(max < num) max = num;
		if(num > 0 && min_pos > num) min_pos = num;
	}
	
	public void close(VDirectory dir, String table, String field) {
        VOutputStream out_freq = dir.create(table + "." + field + ".num.freq");
        out_freq.writeVInt(m_docsCount);
        VOutputStream out_fdoc = dir.create(table + "." + field + ".num.fdoc");
        CompressedNumWriter w_freq = new CompressedNumWriter(out_freq, 16 * 1024);
        CompressedNumWriter w_fdoc = new CompressedNumWriter(out_fdoc, 16 * 1024);

        VOutputStream out_ndoc = null;
        CompressedNumWriter w_ndoc = null;
        if(!m_isSingleValued) {
            out_ndoc = dir.create(table + "." + field + ".num.ndoc");
            w_ndoc = new CompressedNumWriter(out_ndoc, 16 * 1024);
        }

        int pos = 0;
        for(int i = 0; i < m_len.length; i++) {
            int len = m_len[i];
            w_freq.add(len);
            if(len > 0) {
                long num = m_doc.get(pos++);
                w_fdoc.add(num);
                for(int j = 1; j < len; j++) {
                    long nextnum = m_doc.get(pos++);
                    w_ndoc.add(nextnum - num);
                    num = nextnum;
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
			int setCount = m_mask.bitsSet();
			if(setCount == 0) return;
			else if(setCount != m_docsCount) {
				VOutputStream mask_stream = dir.create(table + "." + field + ".num.mask");
				mask_stream.write(m_mask.getBuffer(), 0, m_mask.getBuffer().length);
				mask_stream.close();
			}
			
			long[] values = new long[m_docsCount];
			for(int i = 0; i < m_docsCount; i++) {
				if(!m_mask.get(i)) continue;
				values[i] = m_doc.get(m_pos.get(i));
			}
			
			int size = values.length;
			if(size == 0) return;
			VOutputStream stream = dir.create(table + "." + field + ".dat");
			bits = NumArray.writeArray(values, min, max, stream);
		}else {
			VOutputStream out_dat = dir.create(table + "." + field + ".dat");
			VOutputStream out_pos = dir.create(table + "." + field + ".pos");
			
			long[] values = new long[m_doc.size()];
			for(int i = 0; i < m_doc.size(); i++) {
				values[i] = m_doc.get(i);
			}
			bits = NumArray.writeArray(values, min, max, out_dat);
			
			out_pos.writeVInt(m_pos.size() - 1);
			for(int i = 1; i < m_pos.size(); i++) {
				int start = m_pos.get(i - 1);
				int end = m_pos.get(i);
				out_pos.writeVInt(end - start);
			}
			out_pos.close();
		}
		*/
	}

}
