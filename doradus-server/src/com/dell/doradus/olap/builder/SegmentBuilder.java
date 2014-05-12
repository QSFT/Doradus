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

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapDocument;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.IdWriter;
import com.dell.doradus.olap.store.NumWriter;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.olap.store.ValueWriter;

public class SegmentBuilder {
	public ApplicationDefinition application;
	public Map<String, TableBuilder> tables = new HashMap<String, TableBuilder>();

	public SegmentBuilder(ApplicationDefinition application) {
		this.application = application;
	}

	public void add(OlapBatch batch) {
		for(OlapDocument doc : batch.documents) {
			add(doc);
		}
	}

	public void add(DBObjectBatch dbObjBatch) {
	    for(DBObject dbObj : dbObjBatch.getObjects()) {
	        add(dbObj);
	    }
	}
	
    public void flush(VDirectory dir) {
		SegmentStats stats = new SegmentStats();
		for(TableBuilder b : tables.values()) {
			IdWriter id_writer = new IdWriter(dir, b.table);
			b.documents.flush(id_writer);
			id_writer.close();
			stats.addTable(b.table, id_writer.size());
		}
		for(TableBuilder b : tables.values()) {
			SegmentStats.Table t = stats.getTable(b.table);
			TableDefinition tableDef = application.getTableDef(b.table);
			for(String numField : b.numericFields) {
				NumWriter num_writer = new NumWriter(t.documents);
				b.documents.flush(numField, num_writer);
				num_writer.close(dir, b.table, numField);
				FieldDefinition fieldDef = tableDef.getFieldDef(numField);
				stats.addNumField(fieldDef, num_writer);
			}
			for(FieldBuilder fb : b.fields.values()) {
				ValueWriter term_writer = new ValueWriter(dir, b.table, fb.fieldName);
				fb.flush(term_writer);
				term_writer.close();
				FieldWriter pos_writer = new FieldWriter(t.documents);
				b.documents.flushField(fb.fieldName, pos_writer);
				pos_writer.close(dir, b.table, fb.fieldName);
				FieldDefinition fieldDef = tableDef.getFieldDef(fb.fieldName);
				stats.addTextField(fieldDef, pos_writer);
			}
			for(String link : b.linkFields) {
				FieldWriter pos_writer = new FieldWriter(t.documents);
				b.documents.flushLink(link, pos_writer);
				pos_writer.close(dir, b.table, link);
				FieldDefinition fieldDef = tableDef.getFieldDef(link);
				stats.addLinkField(fieldDef, pos_writer);
			}
		}
		
		stats.totalStoreSize = dir.totalLength(false);
		stats.save(dir);
	}
	
	
	private TableBuilder getTable(String tableName) {
		TableBuilder b = tables.get(tableName);
		if(b == null) {
			b = new TableBuilder(tableName);
			tables.put(b.table, b);
		}
		return b;
	}

	private void add(OlapDocument document) {
		Utils.require(document.id != null, "_ID field is not set for a document");
		TableDefinition tableDef = application.getTableDef(document.table);
		Utils.require(tableDef != null, "Table '" + document.table + "' does not exist");
		TableBuilder b = getTable(document.table);
		Doc doc = b.addDoc(document.id);
		if(document.deleted) doc.deleted = true;
		for(FieldDefinition field : tableDef.getFieldDefinitions()) {
			add(b, doc, field, document);
		}
	}
	
	private void add(DBObject dbObj) {
	    Utils.require(!Utils.isEmpty(dbObj.getObjectID()), "Object is missing '_ID' field");
	    String tableName = dbObj.getFieldValue("_table");
	    Utils.require(!Utils.isEmpty(tableName), "Object is missing '_table' definition");
	    TableDefinition tableDef = application.getTableDef(tableName);
	    Utils.require(tableDef != null, "Unknown table for application '%s': %s", application.getAppName(), tableName);
	    TableBuilder b = getTable(tableDef.getTableName());
	    Doc doc = b.addDoc(dbObj.getObjectID());
	    if("true".equals(dbObj.getFieldValue("_deleted"))) doc.deleted = true;
	    for(FieldDefinition field : tableDef.getFieldDefinitions()) {
	        add(b, doc, field, dbObj);
	    }
	}
	
	private void add(TableBuilder b, Doc doc, FieldDefinition field, OlapDocument document) {
		List<String> f = document.fields.get(field.getName());
		if(f == null || f.size() == 0) return;
		switch(field.getType()) {
		case BOOLEAN:
			if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
			boolean bvalue = "true".equalsIgnoreCase(f.get(0));
			b.addNum(doc, field.getName(), bvalue ? 1 : 0);
			break;
		case GROUP:
			for(FieldDefinition child : field.getNestedFields()) {
				add(b, doc, child, document);
			}
			break;
		case INTEGER:
		case LONG:
			if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
			try {
			    b.addNum(doc, field.getName(), Long.parseLong(f.get(0)));
			} catch (NumberFormatException e) {
			    throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
			}
			break;
		case DOUBLE:
			if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
			try {
				double val = Double.parseDouble(f.get(0));
				long lval = Double.doubleToRawLongBits(val);
			    b.addNum(doc, field.getName(), lval);
			} catch (NumberFormatException e) {
			    throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
			}
			break;
		case FLOAT:
			if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
			try {
				float val = Float.parseFloat(f.get(0));
				int ival = Float.floatToRawIntBits(val);
			    b.addNum(doc, field.getName(), ival);
			} catch (NumberFormatException e) {
			    throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
			}
			break;
		case LINK:
			TableBuilder b2 = getTable(field.getLinkExtent());
			for(String id : f) {
				Doc linkedDoc = b2.addDoc(id);
				b.addLink(doc, field.getName(), linkedDoc);
				b2.addLink(linkedDoc, field.getLinkInverse(), doc);
			}
			break;
		case TEXT:
		case BINARY:
			for(String term : f) {
				b.addTerm(doc, field.getName(), term);
			}
			break;
		case TIMESTAMP:
			if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
			Date dvalue = Utils.dateFromString(f.get(0));
			b.addNum(doc, field.getName(), dvalue.getTime());
			break;
			default: throw new IllegalArgumentException("Unknown Olap type " + field.getType().toString());
		}
	}
	
	private void add(TableBuilder b, Doc doc, FieldDefinition field, DBObject dbObj) {
	    Collection<String> f = dbObj.getFieldValues(field.getName());
	    if(f == null || f.size() == 0) return;
	    switch(field.getType()) {
	    case BOOLEAN:
	        if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
	        boolean bvalue = "true".equalsIgnoreCase(f.iterator().next());
	        b.addNum(doc, field.getName(), bvalue ? 1 : 0);
	        break;
	    case GROUP:
	        for(FieldDefinition child : field.getNestedFields()) {
	            add(b, doc, child, dbObj);
	        }
	        break;
	    case INTEGER:
	    case LONG:
	        if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
	        try {
	            b.addNum(doc, field.getName(), Long.parseLong(f.iterator().next()));
	        } catch (NumberFormatException e) {
	            throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.iterator().next(), e);
	        }
	        break;
        case DOUBLE:
            if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
            try {
                double val = Double.parseDouble(f.iterator().next());
                long lval = Double.doubleToRawLongBits(val);
                b.addNum(doc, field.getName(), lval);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.iterator().next(), e);
            }
            break;
        case FLOAT:
            if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
            try {
                float val = Float.parseFloat(f.iterator().next());
                int ival = Float.floatToRawIntBits(val);
                b.addNum(doc, field.getName(), ival);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.iterator().next(), e);
            }
            break;
	    case LINK:
	        TableBuilder b2 = getTable(field.getLinkExtent());
	        for(String id : f) {
	            Doc linkedDoc = b2.addDoc(id);
	            b.addLink(doc, field.getName(), linkedDoc);
	            b2.addLink(linkedDoc, field.getLinkInverse(), doc);
	        }
	        break;
	    case TEXT:
	    case BINARY:
	        for(String term : f) {
	            b.addTerm(doc, field.getName(), term);
	        }
	        break;
	    case TIMESTAMP:
	        if(f.size() > 1) throw new IllegalArgumentException("Only Text and Link fields can be multi-valued");
	        Date dvalue = Utils.dateFromString(f.iterator().next());
	        b.addNum(doc, field.getName(), dvalue.getTime());
	        break;
	    default: throw new IllegalArgumentException("Unknown Olap type " + field.getType().toString());
	    }
	}
	
}
