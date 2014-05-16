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
import com.dell.doradus.olap.store.NumWriterMV;

public class IdBuilder {
	public Map<BSTR, Doc> docs = new HashMap<BSTR, Doc>();
	private Doc[] list = null;
	private BitVector bvDeleted = null;
	
	public Doc add(BSTR id, int fieldsCount) {
		Doc d = docs.get(id);
		if(d == null) {
			d = new Doc(id, fieldsCount);
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
	
	public void flushNumField(int fieldIndex, NumWriterMV writer) {
		for(Doc d : list) {
			d.flushNumField(fieldIndex, writer);
		}
	}

	public void flushTextField(int fieldIndex, FieldWriter writer) {
		for(Doc d : list) {
			d.flushTextField(fieldIndex, writer);
		}
	}
	
	public void flushLinkField(int fieldIndex, FieldWriter writer) {
		for(Doc d : list) {
			d.flushLinkField(fieldIndex, writer);
		}
	}
	
}
