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

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.service.spider.SpiderHelper;

public class LinkIterator implements Iterator<ObjectID> {
	private FieldDefinition m_link;
	private Integer m_shard;
	private ObjectID m_id;
	private int m_bufferSize;
	private List<ObjectID> m_buffer;
	private int m_next;
	
	public LinkIterator(FieldDefinition link, Integer shard, ObjectID id, int bufferSize, List<ObjectID> buffer) {
		m_link = link;
		m_shard = shard;
		m_id = id;
		m_bufferSize = bufferSize;
		m_buffer = buffer;
		m_next = 0;
	}
	
	@Override
	public boolean hasNext() {
		if(m_next == m_buffer.size()) {
			if(m_buffer.size() != m_bufferSize) return false;
			ObjectID continuation = m_buffer.get(m_buffer.size() - 1);
			m_buffer = SpiderHelper.getLinks(m_link, m_shard, m_id, continuation, false, m_bufferSize);
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
