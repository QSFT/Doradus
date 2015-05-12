/*
 * Copyright (C) 2015 Dell, Inc.
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

package com.dell.doradus.olap;

import com.dell.doradus.olap.OlapBatch.InternalOlapDocument2;
import com.dell.doradus.olap.io.BSTR;

public class OlapDocument {
	private InternalOlapDocument2 m_document;
	
	OlapDocument(InternalOlapDocument2 document) { m_document = document; }

	public OlapDocument addField(String field, String value) {
		m_document.addField(field, value);
		return this;
	}

	public String getTable() {
		String table = m_document.getTable();
		if(table == "") return null;
		return table;
	}
	public String getId() {
		String id = m_document.getId();
		if(id == "") return null;
		return id;
	}
	public void setTable(String table) { m_document.setTable(table); }
	public void setId(String id) { m_document.setId(id); }
	public boolean isDeleted() { return m_document.isDeleted(); }
	public int getFieldsCount() { return m_document.getFieldsCount(); }
	public String getFieldName(int field) { return m_document.getFieldName(field); } 
	public String getFieldValue(int field) { return m_document.getFieldValue(field); } 
	
	public BSTR getIdBinary() { return m_document.getIdBinary(); }
    public BSTR getFieldNameBinary(int field) { return m_document.getFieldNameBinary(field); } 
	public BSTR getFieldValueBinary(int field) { return m_document.getFieldValueBinary(field); } 
	public BSTR getFieldValueBinaryLowercase(int field) { return m_document.getFieldValueBinaryLowercase(field); } 
	
	public OlapDocument setDeleted(boolean deleted) {
		m_document.setDeleted(deleted);
		return this;
	}
	
	//changes the document's index
	void setIndex(int index) {
		m_document.setIndex(index);
	}
}
