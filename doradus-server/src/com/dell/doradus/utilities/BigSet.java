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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.util.HeapSort;

/**
 * Implements a set of byte sequences stored in temporary files.
 * The set may be in READ or WRITE state. In WRITE state it is possible to add
 * new values; when switched to READ state it is not possible to add data anymore.
 * Iteration of data in READ state is in increasing order.
 * Data are deleted completely after the last value is read or the "add" call
 * switches to WRITE state. 
 */
public class BigSet implements Iterable<BSTR> {
	private enum Status {
		READ,	// READ state
		WRITE,	// WRITE state
		NONE	// Initial state (no data)
	}
	
	private Status m_status = Status.NONE;	// Current state
	private final int m_cacheSize;			// Number of records to store in memory
	private final String m_fileName;		// Data files name prefix
	private int m_nextNo = 1;				// Next file to write data into number
	private List<BSTR> m_list;				// Memory buffer
	
	/**
	 * Constructor creates a BigSet with a given file prefix and memory buffer size
	 * (in records). Actual files will get a name &lt;fileName&gt;_number.
	 * 
	 * @param fileName	File name prefix
	 * @param cacheSize	Memory buffer size (in records)
	 */
	public BigSet(String fileName, int cacheSize) {
		m_fileName = fileName;
		m_cacheSize = cacheSize;
	}

	@Override
	public Iterator<BSTR> iterator() {
		openRead();
		return new BigSetIterator();
	}
	
	/**
	 * Adding a new byte sequence to the set.
	 * 
	 * @param value
	 */
	public void add(byte[] value) {
		add(new BSTR(value));
	}

	/**
	 * Adding a string to the set.
	 * @param value
	 */
	public void add(String value) {
		add(new BSTR(value));
	}
	
	/**
	 * Adding a wrapped byte sequence in the set.
	 * @param value
	 */
	public void add(BSTR value) {
		openWrite();
		if(m_list.size() >= m_cacheSize) flushPartial();
		m_list.add(value);
	}
	
	/**
	 * Flushing and closing the files.
	 */
	public void close() {
		if(m_list.size() > 0) {
			flushPartial();
		}
	}
	
	/**
	 * Deleting data files.
	 */
	public void delete() {
		for(int i = 1; i < m_nextNo; i++) {
			new File(m_fileName + "_" + i).delete();
		}
		m_nextNo = 1;
	}
	
	//--------------------- PRIVATE ------------------------
	
	/**
	 * File of BSTRs as Iterable
	 */
	public static class BSTRFileIterable implements Iterable<BSTR> {
		final String m_fileName;
		
		public BSTRFileIterable(String fileName) {
			m_fileName = fileName;
		}
		
		/**
		 * Sequential read iterator implementation
		 */
		public static class BSTRFileIterator implements Iterator<BSTR> {
			private FInputStream m_input;		// File to read from
			private BSTR m_bstr = new BSTR();	// Next BSTR (just read)
			
			public BSTRFileIterator(String fileName) {
				m_input = new FInputStream(fileName);
			}
			
			public void close() { m_input.close(); }

			@Override
			public boolean hasNext() {
				if(m_input == null) return false;
				else if(m_input.end()) {
					m_input.close();
					m_input = null;
					return false;
				} else return true;
			}

			@Override
			public BSTR next() { 
				m_input.readVString(m_bstr);
				return new BSTR(m_bstr);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("remove not supported");
			}
		}
		
		@Override
		public Iterator<BSTR> iterator() {
			return new BSTRFileIterator(m_fileName);
		}
		
	}
	
	private class BigSetIterator implements Iterator<BSTR> {
		BSTR m_last = null;
		Iterator<BSTR> m_iterator;
		
		public BigSetIterator() {
			HeapSort<BSTR> heapSort = new HeapSort<BSTR>(); 
			for(int i = 1; i < m_nextNo; i++) {
				heapSort.add(new BSTRFileIterable(m_fileName + "_" + i));
			}
			
			m_iterator = heapSort.iterator();
			while (m_iterator.hasNext()) {
				BSTR next = m_iterator.next();
				if (!next.equals(m_last)) {
					m_last = next;
					break;
				}
			}
			
		}

		@Override
		public boolean hasNext() {
			return m_last != null;
		}

		@Override
		public BSTR next() {
			BSTR result = m_last;
			while (m_iterator.hasNext()) {
				BSTR next = m_iterator.next();
				if (!next.equals(result)) {
					m_last = next;
					return result;
				}
			}
			m_last = null;
			return result;
		}

		@Override
		public void remove() {
			throw new RuntimeException("remove not supported");
		}
		
	}
	

	private void flushPartial() {
		FOutputStream output = new FOutputStream(m_fileName + "_" + m_nextNo);
		Collections.sort(m_list);
		for(BSTR val : m_list) output.writeVString(val);
		output.close();
		m_nextNo++;
		m_list.clear();
	}

	private void openWrite() {
		if (m_status != Status.WRITE) {
			if (m_status == Status.READ) {
				delete();
			}
			m_list = new ArrayList<BSTR>(m_cacheSize);
			m_status = Status.WRITE;
		}
	}
	
	private void openRead() {
		if (m_status == Status.WRITE) {
			close();
		}
		m_status = Status.READ;
	}
}
