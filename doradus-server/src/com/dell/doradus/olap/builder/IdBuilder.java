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

package com.dell.doradus.olap.builder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.BitVector;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.IdWriter;
import com.dell.doradus.olap.store.NumWriter;

public class IdBuilder {
	public Map<BSTR, Doc> docs = new HashMap<BSTR, Doc>();
	private Doc[] list = null;
	private BitVector bvDeleted = null;
	
	public Doc add(BSTR id) {
		Doc d = docs.get(id);
		if(d == null) {
			d = new Doc(id);
			docs.put(d.id, d);
		}
		return d;
	}
	
	public void flush(IdWriter writer) {
		list = new Doc[docs.size()];
		int c = 0;
		for(Doc doc : docs.values()) {
			list[c++] = doc;
		}
		docs = null;
		//Collections.sort(list, Doc.COMP_ID);
		Arrays.sort(list, Doc.COMP_ID);
		for(int i = 0; i < list.length; i++) {
			Doc doc = list[i];
			doc.number = i;
			writer.add(doc.id);
			if(doc.deleted) {
				if(bvDeleted == null) bvDeleted = new BitVector(list.length);
				bvDeleted.set(i);
			}
		}
		if(bvDeleted != null) writer.setDeletedVector(bvDeleted);
	}
	
	public void flush(String field, NumWriter writer) {
		for(Doc d : list) {
			Long v = d.numerics.get(field);
			if(v != null) writer.add(d.number, v.longValue());
		}
	}

	public void flushField(String field, FieldWriter writer) {
		for(Doc d : list) {
			d.flushField(field, writer);
		}
	}
	
	public void flushLink(String link, FieldWriter writer) {
		for(Doc d : list) {
			d.flushLink(link, writer);
		}
	}
	
}
