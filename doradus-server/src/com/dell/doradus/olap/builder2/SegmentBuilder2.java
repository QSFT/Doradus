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
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.builder2.OlapBatch2.OlapDocument2;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.SegmentStats;

public class SegmentBuilder2 {
	private ApplicationDefinition m_appDef;
	private Map<String, TableBuilder2> m_tables = new HashMap<String, TableBuilder2>();

	public SegmentBuilder2(ApplicationDefinition application) {
		m_appDef = application;
	}

	public void add(OlapBatch2 batch) {
		for(OlapDocument2 doc : batch) {
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

	private void add(OlapDocument2 document) {
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
