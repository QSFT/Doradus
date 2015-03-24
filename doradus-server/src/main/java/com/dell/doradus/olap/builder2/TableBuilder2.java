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

package com.dell.doradus.olap.builder2;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.FieldWriterSV;
import com.dell.doradus.olap.store.IdWriter;
import com.dell.doradus.olap.store.NumWriter;
import com.dell.doradus.olap.store.NumWriterMV;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.olap.store.ValueWriter;

public class TableBuilder2 {
	private IdsBuilder m_ids = new IdsBuilder();
	private Map<String, NumsBuilder> m_nums = new HashMap<>();
	private Map<String, ValuesBuilder> m_values = new HashMap<>();
	private Map<String, TextFieldBuilder> m_fields = new HashMap<>();
	private Map<String, LinkFieldBuilder> m_links = new HashMap<>();
	
	public TableBuilder2() {
		m_ids = new IdsBuilder();
	}

	public IdsBuilder getIds() { return m_ids; }
	
	public int addDoc(BSTR id) {
		return m_ids.add(id);
	}
	
	public void setDeleted(int doc) {
		m_ids.setDeleted(doc, true);
	}
	
	public void addNum(int doc, String field, long value) {
		NumsBuilder b = m_nums.get(field);
		if(b == null) {
			b = new NumsBuilder(m_ids);
			m_nums.put(field, b);
		}
		b.add(doc, value);
	}
	
	public void addTerm(int doc, String field, BSTR term, BSTR orig) {
		ValuesBuilder b = m_values.get(field);
		if(b == null) {
			b = new ValuesBuilder();
			m_values.put(field, b);
		}
		int fld = b.addTerm(term, orig);
		
		TextFieldBuilder t = m_fields.get(field);
		if(t == null) {
			t = new TextFieldBuilder(m_ids, b);
			m_fields.put(field, t);
		}
		t.add(doc, fld);
	}

	public void addLink(int doc, String field, IdsBuilder linked, int linkedDoc) {
		LinkFieldBuilder b = m_links.get(field);
		if(b == null) {
			b = new LinkFieldBuilder(m_ids, linked);
			m_links.put(field, b);
		}
		b.add(doc, linkedDoc);
	}
	
	public void flush(VDirectory dir, SegmentStats stats, TableDefinition tableDef) {
		// flush ids
		String table = tableDef.getTableName();
		IdWriter id_writer = new IdWriter(dir, table);
		m_ids.flush(id_writer);
		id_writer.close();
		int docs_count = id_writer.size();
		stats.addTable(table, docs_count);
		// flush num fields
		for(String field: m_nums.keySet()) {
			FieldDefinition fieldDef = tableDef.getFieldDef(field);
			if(fieldDef.isCollection()) {
				NumWriterMV num_writer = new NumWriterMV(docs_count);
				m_nums.get(field).flush(num_writer);
				num_writer.close(dir, table, field);
				stats.addNumField(fieldDef, num_writer);
			} else {
				NumWriter num_writer = new NumWriter(docs_count);
				m_nums.get(field).flush(num_writer);
				num_writer.close(dir, table, field);
				stats.addNumField(fieldDef, num_writer);
			}
		}
		//flush text field values
		for(String field: m_values.keySet()) {
			ValueWriter term_writer = new ValueWriter(dir, table, field);
			m_values.get(field).flush(term_writer);
			term_writer.close();
		}
		//flush text fields
		for(String field: m_fields.keySet()) {
			FieldDefinition fieldDef = tableDef.getFieldDef(field);
			if(fieldDef.isCollection()) {
				FieldWriter field_writer = new FieldWriter(docs_count);
				m_fields.get(field).flush(field_writer);
				field_writer.close(dir, table, field);
				stats.addTextField(fieldDef, field_writer);
			} else {
				FieldWriterSV field_writer = new FieldWriterSV(docs_count);
				m_fields.get(field).flush(field_writer);
				field_writer.close(dir, table, field);
				stats.addTextField(fieldDef, field_writer);
			}
		}
		
	}

	public void flushLinks(VDirectory dir, SegmentStats stats, TableDefinition tableDef) {
		String table = tableDef.getTableName();
		int docs_count = stats.getTable(table).documents;
		for(String field: m_links.keySet()) {
			FieldDefinition fieldDef = tableDef.getFieldDef(field);
			FieldWriter field_writer = new FieldWriter(docs_count);
			m_links.get(field).flush(field_writer);
			field_writer.close(dir, table, field);
			stats.addLinkField(fieldDef, field_writer);
		}
	}
	
}
