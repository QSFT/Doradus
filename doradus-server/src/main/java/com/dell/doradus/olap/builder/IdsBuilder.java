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

package com.dell.doradus.olap.builder;

import com.dell.doradus.olap.collections.strings.BstrSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.BitVector;
import com.dell.doradus.olap.store.IdWriter;
import com.dell.doradus.olap.store.IntList;

public class IdsBuilder {
	private BstrSet m_ids;
	private IntList m_deleted;
	private int[] m_remap;
	private int[] m_id_to_doc;
	private boolean m_hasDeletions;
	
	public IdsBuilder() {
		m_ids = new BstrSet();
		m_deleted = new IntList();
	}
	
	public int add(BSTR id) {
		int index = m_ids.add(id);
		if(index == m_deleted.size()) m_deleted.add(0);
		return index;
	}

	public void setDeleted(int doc, boolean deleted) {
		m_deleted.set(doc, deleted ? 1 : 0);
		if(deleted) m_hasDeletions = true;
	}

	public int size() {	return m_ids.size(); }
	
	public int id(int index) { return m_remap[index]; }
	public int doc(int id) { return m_id_to_doc[id]; }
	
	public void flush(IdWriter writer) {
		m_remap = m_ids.sort();
		m_id_to_doc = new int[m_remap.length];
		
		for(int i = 0; i < m_id_to_doc.length; i++) {
			int id = m_remap[i];
			m_id_to_doc[id] = writer.add(m_ids.get(id));
		}
		
		if(m_hasDeletions) {
			BitVector v = new BitVector(size());
			for(int i = 0; i < size(); i++) {
				if(m_deleted.get(i) == 1) v.set(m_id_to_doc[i]);
			}
			writer.setDeletedVector(v);
		}
	}

}
