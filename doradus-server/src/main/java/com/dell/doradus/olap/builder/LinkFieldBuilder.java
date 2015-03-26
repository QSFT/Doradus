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

import java.util.Arrays;

import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.IntList;

public class LinkFieldBuilder {
	private IdsBuilder m_ids;
	private IdsBuilder m_links;
	private IntList m_docs;
	private IntList m_values;
	
	public LinkFieldBuilder(IdsBuilder ids, IdsBuilder links) {
		m_ids = ids;
		m_links = links;
		m_docs = new IntList(64);
		m_values = new IntList(64);
	}
	
	public void add(int doc, int val) {
		m_docs.add(doc);
		m_values.add(val);
	}

	public int size() {	return m_docs.size(); }

	
	public void flush(FieldWriter writer) {
		int docs = writer.getDocsCount();
		int[] values = new int[size()];
		int[] offsets = new int[docs + 1];
		for(int i = 0; i < size(); i++) {
			int doc = m_ids.doc(m_docs.get(i));
			offsets[doc + 1]++;
		}
		for(int i = 0; i < docs; i++) {
			offsets[i + 1] += offsets[i];
		}
		for(int i = 0; i < size(); i++) {
			int doc = m_ids.doc(m_docs.get(i));
			int linkeddoc = m_links.doc(m_values.get(i));
			values[offsets[doc]] = linkeddoc;
			offsets[doc]++;
		}
		for(int i = 0; i < docs; i++) {
			int offset = i == 0 ? 0 : offsets[i - 1];
			int length = i == 0 ? offsets[0] : offsets[i] - offsets[i - 1];
			Arrays.sort(values, offset, offset + length);
			for(int j = 0; j < length; j++) {
				writer.add(i, values[offset + j]);
			}
		}
	}
	
}
