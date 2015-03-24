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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.IDGenerator;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapDocument;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.SegmentStats;

public class SegmentBuilder2 {
	private ApplicationDefinition m_appDef;
	private Map<String, TableBuilder2> m_tables = new HashMap<String, TableBuilder2>();
	

	public SegmentBuilder2(ApplicationDefinition application) {
		m_appDef = application;
	}

	public void add(OlapBatch batch) {
		for(OlapDocument doc : batch) {
			add(doc);
		}
	}

	public void add(DBObjectBatch batch) {
		for(DBObject doc : batch.getObjects()) {
			add(doc);
		}
	}
	
    public void flush(VDirectory dir) {
		SegmentStats stats = new SegmentStats();
		for(String table : m_tables.keySet()) {
			TableBuilder2 b = m_tables.get(table);
			b.flush(dir, stats, m_appDef.getTableDef(table));
		}
		for(String table : m_tables.keySet()) {
			TableBuilder2 b = m_tables.get(table);
			b.flushLinks(dir, stats, m_appDef.getTableDef(table));
		}
		stats.totalStoreSize = dir.totalLength(false);
		stats.save(dir);
	}
	
	private TableBuilder2 getTable(TableDefinition tableDef) {
		TableBuilder2 b = m_tables.get(tableDef.getTableName());
		if(b == null) {
			b = new TableBuilder2();
			m_tables.put(tableDef.getTableName(), b);
		}
		return b;
	}

	private void add(OlapDocument document) {
		String table = document.getTable();
		TableDefinition tableDef = m_appDef.getTableDef(table);
		Utils.require(tableDef != null, "Table '" + table + "' does not exist");
		TableBuilder2 b = getTable(tableDef);
		int doc = b.addDoc(document.getIdBinary());
		if(document.isDeleted()) b.setDeleted(doc);
		int fields = document.getFieldsCount();
		for(int i = 0; i < fields; i++) {
			String field = document.getFieldName(i);
			FieldDefinition fieldDef = tableDef.getFieldDef(field);
			Utils.require(fieldDef != null, "Field '" + field + "' does not exist");
		    switch(fieldDef.getType()) {
		    case BOOLEAN: 
		    case INTEGER: 
		    case LONG:
	        case DOUBLE:
	        case FLOAT:
		    case TIMESTAMP:
		    	b.addNum(doc, field, parseNumField(fieldDef, document.getFieldValue(i)));
		    	break;
		    case LINK: {
		        TableBuilder2 b2 = getTable(fieldDef.getInverseTableDef());
		        int linkedDoc = b2.addDoc(document.getFieldValueBinary(i));
	            b.addLink(doc, field, b2.getIds(), linkedDoc);
	            b2.addLink(linkedDoc, fieldDef.getLinkInverse(), b.getIds(), doc);
		        break;
	        }
		    case TEXT: {
		    	BSTR orig = document.getFieldValueBinary(i);
		    	BSTR term = document.getFieldValueBinaryLowercase(i);
	            b.addTerm(doc, field, term, orig);
		        break;
		    }
		    case BINARY: {
	            b.addTerm(doc, field, document.getFieldValueBinary(i), document.getFieldValueBinary(i));
		        break;
		    }
		    default: throw new IllegalArgumentException("Unknown Olap type " + fieldDef.getType().toString());
		    }
		}
	}

	private void add(DBObject document) {
		String table = document.getTableName();
		String id = document.getObjectID();
		if(id == null) id = Utils.base64FromBinary(IDGenerator.nextID());
		TableDefinition tableDef = m_appDef.getTableDef(table);
		Utils.require(tableDef != null, "Table '" + table + "' does not exist");
		TableBuilder2 b = getTable(tableDef);
		int doc = b.addDoc(new BSTR(id));
		if(document.isDeleted()) b.setDeleted(doc);
		for(String field: document.getFieldNames()) {
			FieldDefinition fieldDef = tableDef.getFieldDef(field);
			// DBObject has system fields together with ordinady fields
			if(fieldDef == null) continue;
			//Utils.require(fieldDef != null, "Field '" + field + "' does not exist");
			List<String> values = document.getFieldValues(field);
			for(String value: values) {
			    switch(fieldDef.getType()) {
			    case BOOLEAN: 
			    case INTEGER: 
			    case LONG:
		        case DOUBLE:
		        case FLOAT:
			    case TIMESTAMP:
			    	b.addNum(doc, field, parseNumField(fieldDef, value));
			    	break;
			    case LINK: {
			        TableBuilder2 b2 = getTable(fieldDef.getInverseTableDef());
			        int linkedDoc = b2.addDoc(new BSTR(value));
		            b.addLink(doc, field, b2.getIds(), linkedDoc);
		            b2.addLink(linkedDoc, fieldDef.getLinkInverse(), b.getIds(), doc);
			        break;
		        }
			    case TEXT: {
			    	BSTR orig = new BSTR(value);
			    	BSTR term = new BSTR(value.toLowerCase(Locale.ROOT));
		            b.addTerm(doc, field, term, orig);
			        break;
			    }
			    case BINARY: {
			    	BSTR term = new BSTR(value);
		            b.addTerm(doc, field, term, term);
			        break;
			    }
			    default: throw new IllegalArgumentException("Unknown Olap type " + fieldDef.getType().toString());
			    }
			}
		}
	}
	
	private long parseNumField(FieldDefinition fieldDef, String value) {
		try {
	    switch(fieldDef.getType()) {
	    case BOOLEAN: return "true".equalsIgnoreCase(value) ? 1 : 0;
	    case INTEGER: return Integer.parseInt(value);
	    case LONG: return Long.parseLong(value);
        case DOUBLE: {
            double val = Double.parseDouble(value);
            long lval = Double.doubleToRawLongBits(val);
            return lval;
        }
        case FLOAT: {
            float val = Float.parseFloat(value);
            int ival = Float.floatToRawIntBits(val);
            return ival;
        }
	    case TIMESTAMP: {
	        Date dvalue = Utils.dateFromString(value);
	        return dvalue.getTime();
	    }
	    default: throw new IllegalArgumentException("Not a numeric type " + fieldDef.getType().toString());
	    }
		
		}catch(NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format for field '" + fieldDef.getName() + "': " + value, e);
		}
	}
}
