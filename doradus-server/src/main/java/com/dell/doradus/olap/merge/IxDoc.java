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
import com.dell.doradus.olap.store.IdReader;

public class IxDoc implements Comparable<IxDoc> {
    public int segment;
	public IdReader reader;
    public BSTR id;

    public IxDoc(int segment, IdReader reader)
    {
        this.segment = segment;
        this.reader = reader;
    }

    public void next() {
    	if(reader.next()) id = reader.cur_id;
    	else id = null;
    }

	@Override
	public int compareTo(IxDoc other) {
		if (id == other.id) return 0;
		else if (id == null) return -1;
		else if (other.id == null) return 1;
        int c = other.id.compareTo(id);
        if(c != 0) return c;
        else return other.segment - segment;
	}

}
