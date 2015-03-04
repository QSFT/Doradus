package com.dell.doradus.olap.builder2;

import com.dell.doradus.olap.builder2.OlapBatch2.InternalOlapDocument2;
import com.dell.doradus.olap.io.BSTR;

public class OlapDocument2 {
	private InternalOlapDocument2 m_document;
	
	OlapDocument2(InternalOlapDocument2 document) { m_document = document; }

	public OlapDocument2 addField(String field, String value) {
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
	public BSTR getFieldValueBinary(int field) { return m_document.getFieldValueBinary(field); } 
	public BSTR getFieldValueBinaryLowercase(int field) { return m_document.getFieldValueBinaryLowercase(field); } 
	
	public OlapDocument2 setDeleted(boolean deleted) {
		m_document.setDeleted(deleted);
		return this;
	}
	
}
