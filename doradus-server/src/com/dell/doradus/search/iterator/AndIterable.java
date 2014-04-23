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

public class AndIterable implements Iterable<ObjectID> {
	private List<Iterable<ObjectID>> m_list; 

    public AndIterable(int capacity) {
    	m_list = new ArrayList<Iterable<ObjectID>>(capacity);
    }
    
    public AndIterable(List<? extends Iterable<ObjectID>> list) {
    	m_list = new ArrayList<Iterable<ObjectID>>(list.size());
    	for(Iterable<ObjectID> i : list) add(i);
    }

    public void add(Iterable<ObjectID> iterable) {
    	m_list.add(iterable);
    }
	
	@Override public Iterator<ObjectID> iterator() {
		if(m_list.size() == 0) return NoneIterator.instance;
		else if(m_list.size() == 1) return m_list.get(0).iterator();

		List<Iterator<ObjectID>> list = new ArrayList<Iterator<ObjectID>>(m_list.size());
		for(Iterable<ObjectID> i : m_list) list.add(i.iterator());
		return new AndIterator(list);
	}
}
