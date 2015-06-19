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

package com.dell.doradus.olap;

import java.io.Reader;
import java.util.Iterator;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.IDGenerator;
import com.dell.doradus.olap.builder.BatchBuilder;
import com.dell.doradus.olap.builder.SegmentBuilder;
import com.dell.doradus.olap.collections.strings.StringList;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.IntList;

public class OlapBatch implements Iterable<OlapDocument> {
	private StringList m_data;
	private IntList m_deleted;
	private IntList m_docOffsets;

	public OlapBatch() {
		m_data = new StringList();
		m_deleted = new IntList(64);
		m_docOffsets = new IntList(64);
	}
	
	// Use "_ID" as the ID field name
	public static OlapBatch parseJSON(String text) { return BatchBuilder.parseJSON(text); }
	public static OlapBatch parseJSON(Reader reader) { return BatchBuilder.parseJSON(reader); }
	public static OlapBatch fromUNode(UNode rootNode) { return BatchBuilder.fromUNode(rootNode); }
	
	// Use custom id name field
	public static OlapBatch parseJSON(String text, String idNameField) { return BatchBuilder.parseJSON(text, idNameField); }
	public static OlapBatch parseJSON(Reader reader, String idNameField) { return BatchBuilder.parseJSON(reader, idNameField); }
	public static OlapBatch fromUNode(UNode rootNode, String idNameField) { return BatchBuilder.fromUNode(rootNode, idNameField); }
	
	
	public OlapDocument addDoc() { return addDoc(null, null); }
	public OlapDocument addDoc(String table, String id) {
	    if(id == null) id = Utils.base64FromBinary(IDGenerator.nextID());
	    m_docOffsets.add(m_data.size());
		m_data.add(table);
		m_data.add(id);
		m_deleted.add(0);
		return new OlapDocument(new InternalOlapDocument2(size() - 1));
	}
	
	public void clear() {
		m_data.clear();
		m_deleted.clear();
		m_docOffsets.clear();
	}
	public int size() { return m_docOffsets.size(); }
	
	public OlapDocument get(int index) { return new OlapDocument(new InternalOlapDocument2(index)); }
	
	
	@Override public Iterator<OlapDocument> iterator() { return new DocIterator(); }
	
	
	public void flushSegment(ApplicationDefinition application, VDirectory directory) {
		SegmentBuilder builder = new SegmentBuilder(application);
		builder.add(this);
		builder.flush(directory);
	}

	
	class InternalOlapDocument2 {
		private int m_index;
		private int m_offset;
		
		InternalOlapDocument2(int index) { setIndex(index); }
		
		public void addField(String field, String value) {
			if(m_index < size() - 1) throw new RuntimeException("Fields can be added only to the last added document");
			if(value == null) return;
			m_data.add(field);
			m_data.add(value);
		}
		
		private int data(int field) { return m_offset + field * 2; }
		
		public String getTable() { return m_data.get(data(0)); }
		public String getId() { return m_data.get(data(0) + 1); }
		public void setTable(String table) { m_data.set(data(0), table); }
		public void setId(String id) { m_data.set(data(0) + 1, id); }
		
		public boolean isDeleted() { return m_deleted == null ? false : m_deleted.get(m_index) == 1; }
		public int getFieldsCount() {
			if(m_index == size() - 1) return (m_data.size() - m_docOffsets.get(m_index) - 2) / 2;
			else return (m_docOffsets.get(m_index + 1) - m_docOffsets.get(m_index) - 2) / 2;
		}
		public String getFieldName(int field) { return m_data.get(data(field + 1)); } 
		public String getFieldValue(int field) { return m_data.get(data(field + 1) + 1); } 
		
		public BSTR getIdBinary() { return m_data.getBinary(data(0) + 1); }
        public BSTR getFieldNameBinary(int field) { return m_data.getBinary(data(field + 1)); } 
		public BSTR getFieldValueBinary(int field) { return m_data.getBinary(data(field + 1) + 1); } 
		public BSTR getFieldValueBinaryLowercase(int field) { return m_data.getBinaryLowercase(data(field + 1) + 1); } 
		
		public void setDeleted(boolean deleted) { m_deleted.set(m_index, deleted ? 1 : 0); }
		public void setIndex(int index) {  m_index = index; m_offset = m_docOffsets.get(m_index); }
		
	}

	public class DocIterator implements Iterator<OlapDocument> {
		private int m_next;
		@Override public boolean hasNext() { return m_next < size(); }
		@Override public OlapDocument next() { return get(m_next++); }
		@Override public void remove() { throw new RuntimeException("Not Implemented"); }
		
	}

}
