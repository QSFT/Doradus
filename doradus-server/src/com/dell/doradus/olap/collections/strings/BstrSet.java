/*
 * Copyright (C) 2015 Dell, Inc.
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

package com.dell.doradus.olap.collections.strings;

import com.dell.doradus.olap.collections.IntSet;
import com.dell.doradus.olap.io.BSTR;

public class BstrSet {
	private BstrList m_list;
	private IntSet m_intSet;
	
	public BstrSet() {
		m_list = new BstrList();
		m_intSet = new IntSet(m_list, 64);
	}
	
	public int add(BSTR value) {
		int index = m_intSet.indexOf(value);
		if(index < 0) {
			index = m_list.add(value);
			int index2 = m_intSet.add(index);
			if(index != index2) throw new RuntimeException("BstrMap inconsistent");
		}
		return index;
	}

	public BSTR get(int index) {
		return m_list.get(index);
	}
	
	public int size() { return m_list.size(); }
	
	public int[] sort() {
		return m_list.sort();
	}
	
	public void sort(int[] array, int length) {
		m_list.sort(array, length);
	}
	

}
