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

package com.dell.doradus.search.iterator;

import java.util.Iterator;
import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.service.spider.SpiderHelper;

public class TermIterator implements Iterator<ObjectID> {
	private TableDefinition m_table;
	private Integer m_shard;
	private String m_term;
	private int m_bufferSize;
	private List<ObjectID> m_buffer;
	private int m_next;
	
	public TermIterator(TableDefinition table, Integer shard, String term, int bufferSize, List<ObjectID> buffer) {
		m_table = table;
		m_shard = shard;
		m_term = term;
		m_bufferSize = bufferSize;
		m_buffer = buffer;
		m_next = 0;
	}
	
	@Override
	public boolean hasNext() {
		if(m_next == m_buffer.size()) {
			if(m_buffer.size() != m_bufferSize) return false;
			ObjectID continuation = m_buffer.get(m_buffer.size() - 1);
			m_buffer = SpiderHelper.getTermDocs(m_table, m_shard, m_term, continuation, false, m_bufferSize);
			m_next = 0;
		} 
		return m_next < m_buffer.size();
	}

	@Override
	public ObjectID next() {
		if(!hasNext()) throw new RuntimeException("Read past the end of the iterator");
		return m_buffer.get(m_next++);
	}

	@Override public void remove() { throw new RuntimeException("Remove is not supported"); }

}
