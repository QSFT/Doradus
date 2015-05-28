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

package com.dell.doradus.logservice.store;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.collections.strings.BstrSet;
import com.dell.doradus.olap.io.BSTR;

public class FieldBuilder {
    private BSTR m_field;
    private BstrSet m_values;
	private int[] m_docs;
	
	public FieldBuilder(BSTR field, int size) {
	    m_field = field;
		m_docs = new int[size];
		m_values = new BstrSet();
		int index = m_values.add(BSTR.EMPTY);
		if(index != 0) throw new RuntimeException("Error insertin empty string");
	}
	
	public void add(int doc, BSTR value) {
	    int index = m_values.add(value);
		m_docs[doc] = index;
	}

	public void flush(MemoryStream output, Temp temp) {
        output.writeString(m_field);
        int fields_count = m_values.size();
        output.writeVInt(fields_count);
        //1. write dictionary
        int[] remap = m_values.sort();
        MemoryStream s_pfx = temp.getStream(0);
        MemoryStream s_sfx = temp.getStream(1);
        MemoryStream s_len = temp.getStream(2);
        MemoryStream s_dat = temp.getStream(3);
        BSTR last = temp.getBSTR();

        int[] m_doc_to_val = new int[remap.length];
        
        for(int i = 0; i < fields_count; i++) {
            m_doc_to_val[remap[i]] = i;
            BSTR next = m_values.get(remap[i]);
            int l = Math.min(last.length, next.length);
            int pfx = 0;
            while(pfx < l && last.buffer[pfx] == next.buffer[pfx]) pfx++;
            int sfx = 0;
            while(sfx < l - pfx && last.buffer[last.length - sfx - 1] == next.buffer[next.length - sfx - 1]) sfx++;
            s_pfx.writeVInt(pfx);
            s_sfx.writeVInt(sfx);
            s_len.writeVInt(next.length);
            s_dat.write(next.buffer, pfx, next.length - sfx - pfx);
            last.set(next);
        }
        
        Temp.writeCompressed(output, s_pfx);
        Temp.writeCompressed(output, s_sfx);
        Temp.writeCompressed(output, s_len);
        Temp.writeCompressed(output, s_dat);
        
        //2. Write indexes
        MemoryStream s_lst = temp.getStream(0);
        MemoryStream s_fst = temp.getStream(1);
        
        for(int i = 0; i < m_docs.length; i++) {
            int index = m_doc_to_val[m_docs[i]];
            s_lst.writeByte((byte)index);
            if(fields_count >= 256) {
                s_fst.writeVInt(index >> 8);
            }
        }
        Temp.writeCompressed(output, s_lst);
        if(fields_count >= 256) {
            Temp.writeCompressed(output, s_fst);
        }
	}

}
