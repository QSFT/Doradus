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

import com.dell.doradus.core.ObjectID;
import com.dell.doradus.search.util.DefaultComparator;
import com.dell.doradus.search.util.HeapList;

public class OrIterator implements Iterator<ObjectID> {
	private DefaultComparator<ObjectID> m_comp = new DefaultComparator<ObjectID>();
    private HeapList<IxV> m_heap;
    private IxV m_current;
	
    public OrIterator(List<Iterator<ObjectID>> iterators) {
    	m_heap = new HeapList<IxV>(iterators.size() - 1);
        for(Iterator<ObjectID> i: iterators) {
        	IxV ixv = new IxV(i);
        	ixv.next();
        	m_current = m_heap.AddEx(ixv);
        }
    }

	@Override public boolean hasNext() {
		return m_current.V != null;
	}

	@Override public ObjectID next() {
        ObjectID oldval = m_current.V;
        while (m_comp.compare(m_current.V, oldval) == 0) {
        	m_current.next();
            m_current = m_heap.AddEx(m_current);
        }
        return oldval;
	}

	@Override public void remove() { throw new RuntimeException("Cannot remove"); }
    
    class IxV implements Comparable<IxV> {
        public Iterator<ObjectID> I;
        public ObjectID V;

        public IxV() { }
        public IxV(Iterator<ObjectID> i) { I = i; }

        public void next() {
        	V = I.hasNext() ? I.next() : null;
        }
        
        //reverse compare
        public int compareTo(IxV other) {
            return m_comp.compare(other.V, V);
        }
    }


}
