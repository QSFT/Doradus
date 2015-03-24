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

import com.dell.doradus.core.ObjectID;

public class AndNotIterator implements Iterator<ObjectID> {
    private Iterator<ObjectID> m_plus;
    private Iterator<ObjectID> m_minus;
    private ObjectID m_next_plus;
    private ObjectID m_next_minus;
	
    public AndNotIterator(Iterator<ObjectID> plus, Iterator<ObjectID> minus) {
    	m_plus = plus;
    	m_minus = minus;
    	m_next_minus = m_minus.hasNext() ? m_minus.next() : null;
        move();
    }
    
	@Override public boolean hasNext() {
		return m_next_plus != null;
	}

	@Override public ObjectID next() {
		ObjectID next = m_next_plus;
		if(next == null) throw new RuntimeException("Read past the end of the iterator");
		move();
		return next;
	}

	@Override public void remove() { throw new RuntimeException("Cannot remove"); }

	private void move() {
		do {
			if(!m_plus.hasNext()) {
				m_next_plus = null;
				return;
			}
			m_next_plus = m_plus.next();
			while(m_next_minus != null && m_next_minus.compareTo(m_next_plus) < 0) {
				m_next_minus = m_minus.hasNext() ? m_minus.next() : null;
			}
		} while(m_next_plus.equals(m_next_minus));
	}
	
}
