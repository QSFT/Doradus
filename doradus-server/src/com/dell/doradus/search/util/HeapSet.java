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

package com.dell.doradus.search.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HeapSet<T extends Comparable<T>> {
    private HeapList<T> m_heap;
    private Set<T> m_set;

    public HeapSet(int capacity)
    {
    	m_heap = new HeapList<T>(capacity);
    	m_set = new HashSet<T>(capacity);
    }
    
    public int getCapacity() {
        return m_heap.getCapacity();
    }

    public void Put(T value) {
    	if(!m_set.contains(value)) {
    		T old = m_heap.AddEx(value);
    		if(old != null) m_set.remove(old);
    		if(old != value) m_set.add(value);
    	}
    }

    public List<T> GetValues() {
    	List<T> list = new ArrayList<T>(m_set.size());
    	list.addAll(m_set);
    	Collections.sort(list);
        return list;
    }

}
