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

import com.dell.doradus.olap.store.NumSearcherMV;

public class IxNum implements Comparable<IxNum> {
    public int segment;
    public int doc;
    public long num;
    
    private Remap docRemap;
    private NumSearcherMV searcher;
    
    private int num_count;
    private int nxt_doc;
    private int nxt_index;

    public IxNum(int segment, Remap docRemap, NumSearcherMV searcher)
    {
        this.segment = segment;
        this.docRemap = docRemap;
        this.searcher = searcher;
    }

    public void next() {
    	do {
    		doNext();
    	} while(doc != Integer.MAX_VALUE && num < 0);
    }
    
    private void doNext() {
    	if(nxt_index < num_count) {
    		num = searcher.get(nxt_doc - 1, nxt_index++);
    		return;
    	}
    	
    	nxt_index = 0;
    	num_count = 0;
    	
    	while(nxt_doc < searcher.size() && num_count == 0) {
			int d = nxt_doc++;
			doc = docRemap.get(segment, d);
			if(doc < 0) continue;
			num_count = searcher.size(d);
    	}
    	
    	if(num_count == 0) {
    		doc = Integer.MAX_VALUE;
    		num = Long.MAX_VALUE;
    		return;
    	}

		num = searcher.get(nxt_doc - 1, nxt_index++);
		return;
    }

	@Override public int compareTo(IxNum other) {
		if(doc > other.doc) return -1;
		else if(doc < other.doc) return 1;
		else if(num != other.num) return other.num > num ? 1 : -1;
		else return other.segment - segment;
	}

}
