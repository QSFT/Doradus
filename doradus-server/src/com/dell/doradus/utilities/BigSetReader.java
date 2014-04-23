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

package com.dell.doradus.utilities;

import java.util.Iterator;

import com.dell.doradus.olap.io.BSTR;

public class BigSetReader implements Iterable<BSTR> {
	private String m_fileName;
	
	public BigSetReader(String fileName) {
		m_fileName = fileName; 
	}
	
	@Override public Iterator<BSTR> iterator() { return new BigSetIterator(); }
	
	public class BigSetIterator implements Iterator<BSTR> {
		private FInputStream m_input;
		private BSTR m_bstr = new BSTR();
		
		public BigSetIterator() {
			m_input = new FInputStream(m_fileName);
		}
		
		public void close() { m_input.close(); }

		@Override public boolean hasNext() {
			if(m_input == null) return false;
			else if(m_input.end()) {
				m_input.close();
				m_input = null;
				return false;
			} else return true;
		}

		@Override public BSTR next() { 
			m_input.readVString(m_bstr);
			return new BSTR(m_bstr);
		}

		@Override public void remove() { throw new RuntimeException("remove not supported"); }
	}

}
