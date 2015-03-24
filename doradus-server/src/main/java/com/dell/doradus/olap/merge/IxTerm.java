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

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.ValueReader;

public class IxTerm implements Comparable<IxTerm> {
    public int segment;
	public ValueReader reader;
    public BSTR term;
    public BSTR orig;

    public IxTerm(int segment, ValueReader reader)
    {
        this.segment = segment;
        this.reader = reader;
    }

    public void next() {
    	if(reader.next()) {
    		term = reader.cur_term;
    		orig = reader.cur_orig;
    	} else {
    		term = null;
    		orig = null;
    	}
    }

	@Override
	public int compareTo(IxTerm other) {
		if (term == other.term) return 0;
		else if (term == null) return -1;
		else if (other.term == null) return 1;
		int c = other.term.compareTo(term);
		if(c != 0) return c;
		else return other.segment - segment;
	}

}
