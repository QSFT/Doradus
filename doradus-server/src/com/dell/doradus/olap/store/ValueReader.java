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

public class ValueReader {
	private VInputStream m_stream_term;
	private VInputStream m_stream_orig;
	public int cur_number;
	public BSTR cur_term = new BSTR();
	public BSTR cur_orig = new BSTR();
	
	public ValueReader(VDirectory dir, String table, String field) {
		if(!dir.fileExists(table + "." + field + ".term")) return;
		m_stream_term = dir.open(table + "." + field + ".term");
		m_stream_orig = dir.open(table + "." + field + ".orig");
		reset();
	}

	public boolean end() {
		return m_stream_term == null || m_stream_term.end();
	}
	
	public boolean next() {
		if(end()) return false;
		cur_number++;
		m_stream_term.readVString(cur_term);
		m_stream_orig.readVString(cur_orig);
		return true;
	}
	
	public void reset() {
		cur_number = -1;
		cur_term.length = -1;
		cur_orig.length = -1;
		if(m_stream_term == null) return;
		m_stream_term.seek(0);
	}
	
}
