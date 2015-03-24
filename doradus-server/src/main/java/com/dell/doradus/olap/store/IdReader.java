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

public class IdReader {
	private VInputStream m_stream_id;
	private BitVector m_deleted;
	public int cur_number;
	public boolean is_deleted;
	public BSTR cur_id = new BSTR();
	
	public IdReader(VDirectory dir, String table) {
		if(!dir.fileExists(table + "._id")) return;
		
		if(dir.fileExists(table + "._id.deleted")) {
			VInputStream del_stream = dir.open(table + "._id.deleted");
			int size = del_stream.readVInt();
			m_deleted = new BitVector(size);
			del_stream.read(m_deleted.getBuffer(), 0, m_deleted.getBuffer().length);  
		}
		
		m_stream_id = dir.open(table + "._id");
		reset();
	}

	public boolean end() {
		return m_stream_id == null || m_stream_id.end();
	}
	
	public boolean next() {
		if(end()) return false;
		cur_number++;
		m_stream_id.readVString(cur_id);
		if(m_deleted != null) is_deleted = m_deleted.get(cur_number);
		return true;
	}
	
	public void reset() {
		cur_number = -1;
		cur_id.length = -1;
		is_deleted = false;
		if(m_stream_id == null) return;
		m_stream_id.seek(0);
	}
	
}
