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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.core.ObjectID;

public class AndIterator implements Iterator<ObjectID> {
    private List<IxT> m_iterators;
    private ObjectID m_next;
	
    public AndIterator(List<Iterator<ObjectID>> iterators) {
    	m_iterators = new ArrayList<IxT>(iterators.size());
        for(Iterator<ObjectID> i: iterators) {
        	m_iterators.add(new IxT(i));
        }
        move();
    }

	@Override public boolean hasNext() {
		return m_next != null;
	}

	@Override public ObjectID next() {
		ObjectID next = m_next;
		if(next == null) throw new RuntimeException("Read past the end of the iterator");
		move();
		return next;
	}

	@Override public void remove() { throw new RuntimeException("Cannot remove"); }

	private void move() {
		//1. advance
		for(IxT v: m_iterators) {
			do {
				if(!v.I.hasNext()) {
					m_next = null;
					return;
				}
				v.C = v.I.next();
				if(m_next == null) m_next = v.C;
			} while(v.C == null || m_next.compareTo(v.C) > 0);
			m_next = v.C;
		}
		//2. converge
		while(!m_next.equals(m_iterators.get(0).C)) {
			for(IxT v: m_iterators) {
				while(m_next.compareTo(v.C) > 0) {
					if(!v.I.hasNext()) {
						m_next = null;
						return;
					}
					v.C = v.I.next();
					if(m_next == null) m_next = v.C;
				}
				m_next = v.C;
			}
		}
	}
	
	class IxT {
		public Iterator<ObjectID> I;
		public ObjectID C;
		
		public IxT(Iterator<ObjectID> i) {
			I = i;
		}
	}
	
}
