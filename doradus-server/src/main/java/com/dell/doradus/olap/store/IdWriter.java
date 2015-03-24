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

public class IdWriter {
	public static int SPAN = 1024;
	private VDirectory m_dir;
	private String m_table;
	private VOutputStream m_stream_id;
	private VOutputStream m_stream_idx;
	private int m_documents;
	private BSTR m_last = new BSTR();
	private long m_last_position = 0;
	
	public IdWriter(VDirectory dir, String table) {
		m_dir = dir;
		m_table = table;
		m_stream_id = dir.create(table + "._id");
		m_stream_idx = dir.create(table + ".idx");
		m_documents = 0;
		m_last.length = -1;
	}
	
	public int add(BSTR id) {
		if(!BSTR.isEqual(m_last, id)) {
			flush();
			m_last.set(id);
		}
		return m_documents;
	}
	
	public void removeLastId(BSTR id) {
		if(m_last.length != -1 && !BSTR.isEqual(id,  m_last)) throw new RuntimeException("Wrong canceled ID specified");
		m_last.length = -1;
	}
	
	private void flush() {
		if(m_last.length < 0) return;
		if(m_documents % SPAN == 0) {
			m_stream_idx.write(m_last);
			long new_position = m_stream_id.position();
			m_stream_idx.writeVLong(new_position - m_last_position);
			m_last_position = new_position;
		}
		m_stream_id.writeVString(m_last);
		m_last.length = -1;
		m_documents++;
	}

	public void setDeletedVector(BitVector bvDeleted) {
		VOutputStream del_stream = m_dir.create(m_table + "._id.deleted");
		del_stream.writeVInt(bvDeleted.size());
		del_stream.write(bvDeleted.getBuffer(), 0, bvDeleted.getBuffer().length);
		del_stream.close();
	}
	
	public int size() { return m_documents + (m_last.length == -1 ? 0 : 1); }
	
	public void close() {
		flush();
		m_stream_id.close();
		m_stream_idx.close();
	}
	
}
