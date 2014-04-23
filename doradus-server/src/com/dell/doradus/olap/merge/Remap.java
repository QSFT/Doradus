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

package com.dell.doradus.olap.merge;

import com.dell.doradus.olap.store.IntList;

public class Remap {
    private IntList[] m_maps;
    private int m_dstSize = -1;

    public Remap(int count)
    {
        m_maps = new IntList[count];
        for (int i = 0; i < count; i++) m_maps[i] = new IntList();
    }

    public void set(int segment, int srcDoc, int dstDoc)
    {
    	IntList list = m_maps[segment];
        if (list.size() == srcDoc + 1)
        {
            if (list.get(srcDoc) != dstDoc) throw new RuntimeException("Map " + segment + " has last number " + list.get(segment) + "; required " + dstDoc);
            return;
        }
        if (list.size() != srcDoc) throw new RuntimeException("Map " + segment + " has " + list.size() + " elements; required " +srcDoc);
        list.add(dstDoc);
    }

    public void setDeleted(int segment, int srcDoc, int dstDoc)
    {
    	for(; segment >= 0; segment--) {
	    	IntList list = m_maps[segment];
	    	int len = list.size();
	    	if(len == 0) continue;
	        if(list.get(len - 1) == dstDoc) list.set(len - 1, -1);
    	}
    }
    
    public int segmentsCount()  { return m_maps.length; }

    public int get(int segment, int doc)
    {
        return m_maps[segment].get(doc);
    }

    public int size(int segment) { return m_maps[segment].size(); }
    
    public int dstSize() {
    	if(m_dstSize >= 0) return m_dstSize;
    	int lastDoc = -1;
    	for(int i = 0; i < m_maps.length; i++) {
    		IntList map = m_maps[i];
    		int len = map.size();
    		while(len > 0) {
    			len--;
    			int doc = map.get(len);
    			if(doc < 0) continue;
        		if(lastDoc < doc) lastDoc = doc;
        		break;
    		}
    	}
    	m_dstSize = lastDoc + 1;
    	return m_dstSize;
    }
    
    public void shrink() {
    	for(IntList list : m_maps) list.shrinkToSize();
    }
}
