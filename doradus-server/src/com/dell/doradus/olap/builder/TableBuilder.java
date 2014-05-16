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

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.olap.io.BSTR;

public class TableBuilder {
	public static enum FType {
		NUMERIC,
		TEXT,
		LINK
	}
	
	public String table;
	public IdBuilder documents = new IdBuilder();
	public Map<String, Integer> fieldIndexMap = new HashMap<String, Integer>();
	public FieldBuilder[] fieldBuilders;
	public FType[] fieldTypes;
	public String[] fieldNames;
	public int fieldsCount;
	
	private BSTR bstr = new BSTR();
	private BSTR orig = new BSTR();
	
	public TableBuilder(String tableName, int fieldsCount) {
		this.table = tableName;
		this.fieldsCount = fieldsCount;
		this.fieldBuilders = new FieldBuilder[fieldsCount];  
		this.fieldTypes = new FType[fieldsCount];  
		this.fieldNames = new String[fieldsCount];  
	}

	public Doc addDoc(String id) {
		bstr.set(id);
		return documents.add(bstr, fieldsCount);
	}
	
	private int getFieldIndex(String field) {
		Integer findex = fieldIndexMap.get(field);
		if(findex == null) {
			findex = new Integer(fieldIndexMap.size());
			fieldIndexMap.put(field, findex);
		}
		return findex.intValue(); 
	}
	
	public void addNum(Doc doc, String field, long value) {
		int fieldIndex = getFieldIndex(field);
		fieldTypes[fieldIndex] = FType.NUMERIC;
		fieldNames[fieldIndex] = field;
		
		doc.addNumField(fieldIndex, value);
	}

	public void addTerm(Doc doc, String field, String term) {
		int fieldIndex = getFieldIndex(field);
		fieldTypes[fieldIndex] = FType.TEXT;
		fieldNames[fieldIndex] = field;
		
		FieldBuilder b = fieldBuilders[fieldIndex];
		if(b == null) b = fieldBuilders[fieldIndex] = new FieldBuilder(field);
		orig.set(term);
		bstr.set(term.toLowerCase());
		Term t = b.add(bstr, orig);
		doc.addTextField(fieldIndex, t);
	}

	public void addLink(Doc doc, String field, Doc linkedDoc) {
		int fieldIndex = getFieldIndex(field);
		fieldTypes[fieldIndex] = FType.LINK;
		fieldNames[fieldIndex] = field;
		
		doc.addLinkField(fieldIndex, linkedDoc);
	}

}
