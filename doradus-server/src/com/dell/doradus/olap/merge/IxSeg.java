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

import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;

public class IxSeg implements Comparable<IxSeg> {
    public int segment;
    public int doc;
    public int val;
    
    private Remap docRemap;
    private Remap valRemap;
    private FieldSearcher searcher;
    
    private IntIterator iter = new IntIterator();
    private int nxt_doc;
    private int nxt_val;

    public IxSeg(int segment, Remap docRemap, Remap valRemap, FieldSearcher searcher)
    {
        this.segment = segment;
        this.docRemap = docRemap;
        this.valRemap = valRemap;
        this.searcher = searcher;
    }

    public void next() {
    	if(nxt_val < iter.count()) {
    		int v = iter.get(nxt_val++);
    		val = valRemap.get(segment, v);
    		return;
    	}
    	
    	nxt_val = 0;
    	iter.setup(null, 0, 0);
    	
    	while(nxt_doc < searcher.size() && iter.count() == 0) {
			int d = nxt_doc++;
			doc = docRemap.get(segment, d);
			if(doc < 0) continue;
			searcher.fields(d, iter);
    	}
    	
    	if(iter.count() == 0) {
    		doc = Integer.MAX_VALUE;
    		val = 0;
    		return;
    	}

		int v = iter.get(nxt_val++);
		val = valRemap.get(segment, v);
		return;
    }

	@Override
	public int compareTo(IxSeg other) {
		if(doc > other.doc) return -1;
		else if(doc < other.doc) return 1;
		else return other.segment - segment;
	}

}
