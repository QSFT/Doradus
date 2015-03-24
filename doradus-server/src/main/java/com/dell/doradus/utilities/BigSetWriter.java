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
import java.util.List;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.util.HeapSort;

public class BigSetWriter {
	private int m_cacheSize; 
	private String m_filePrefix;
	private int m_nextNo = 1;
	
	private List<BSTR> m_list;
	
	public BigSetWriter(String fileName, int cacheSize) {
		m_cacheSize = cacheSize;
		if(new File(fileName).exists()) new File(fileName).delete();
		int num = 1;
		while(new File(fileName + "_" + num).exists()) {
			new File(fileName + "_" + num).delete();
			num++;
		}
		m_filePrefix = fileName;
		m_list = new ArrayList<BSTR>(m_cacheSize);
	}
	
	public void add(byte[] value) {
		add(new BSTR(value));
	}

	public void add(String value) {
		add(new BSTR(value));
	}
	
	public void add(BSTR value) {
		if(m_list.size() >= m_cacheSize) flushPartial();
		m_list.add(value);
	}
	
	private void flushPartial() {
		FOutputStream output = new FOutputStream(m_filePrefix + "_" + m_nextNo);
		Collections.sort(m_list);
		for(BSTR val : m_list) output.writeVString(val);
		output.close();
		m_nextNo++;
		m_list.clear();
	}

	public void close() {
		if(m_list.size() > 0) {
			flushPartial();
		}
		
		HeapSort<BSTR> heapSort = new HeapSort<BSTR>(); 
		for(int i = 1; i < m_nextNo; i++) {
			heapSort.add(new BigSetReader(m_filePrefix + "_" + i));
		}
		
		BSTR last = new BSTR();
		last.length = -1;
		FOutputStream output = new FOutputStream(m_filePrefix);
		for(BSTR val : heapSort) {
			if(last.equals(val)) continue;
			output.writeVString(val);
			last.set(val);
		}
		output.close();
		for(int i = 1; i < m_nextNo; i++) {
			new File(m_filePrefix + "_" + i).delete();
		}
		
	}
}
