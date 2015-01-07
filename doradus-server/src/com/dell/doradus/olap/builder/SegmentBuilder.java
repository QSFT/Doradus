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
import com.dell.doradus.core.IDGenerator;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapDocument;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.IdWriter;
import com.dell.doradus.olap.store.NumWriterMV;
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
			for(int fieldIndex = 0; fieldIndex < b.fieldsCount; fieldIndex++) {
				String fieldName = b.fieldNames[fieldIndex];
				TableBuilder.FType type = b.fieldTypes[fieldIndex];
				if(type == null) continue;
				switch(type) {
				case NUMERIC: {
					NumWriterMV num_writer = new NumWriterMV(t.documents);
					b.documents.flushNumField(fieldIndex, num_writer);
					num_writer.close(dir, b.table, fieldName);
					FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
					stats.addNumField(fieldDef, num_writer);
					break;
				}
				case TEXT: {
					ValueWriter term_writer = new ValueWriter(dir, b.table, fieldName);
					b.fieldBuilders[fieldIndex].flush(term_writer);
					term_writer.close();
					FieldWriter pos_writer = new FieldWriter(t.documents);
					b.documents.flushTextField(fieldIndex, pos_writer);
					pos_writer.close(dir, b.table, fieldName);
					FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
					stats.addTextField(fieldDef, pos_writer);
					break;
				}
				case LINK: {
					FieldWriter pos_writer = new FieldWriter(t.documents);
					b.documents.flushLinkField(fieldIndex, pos_writer);
					pos_writer.close(dir, b.table, fieldName);
					FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
					stats.addLinkField(fieldDef, pos_writer);
					break;
				}
				default: throw new RuntimeException("Invalid field type");
				}
			}
		}
		
		stats.totalStoreSize = dir.totalLength(false);
		stats.save(dir);
	}
	
	
	private TableBuilder getTable(TableDefinition tableDef) {
		TableBuilder b = tables.get(tableDef.getTableName());
		if(b == null) {
			b = new TableBuilder(tableDef.getTableName(), tableDef.getFieldDefinitions().size());
			tables.put(b.table, b);
		}
		return b;
	}

	private void add(OlapDocument document) {
		if (document.id == null) {
		    document.id = Utils.base64FromBinary(IDGenerator.nextID());
		}
		TableDefinition tableDef = application.getTableDef(document.table);
		Utils.require(tableDef != null, "Table '" + document.table + "' does not exist");
		TableBuilder b = getTable(tableDef);
		Doc doc = b.addDoc(document.id);
		if(document.deleted) doc.deleted = true;
		for(FieldDefinition field : tableDef.getFieldDefinitions()) {
			add(b, doc, field, document.fields.get(field.getName()));
		}
	}
	
	private void add(DBObject dbObj) {
	    if (Utils.isEmpty(dbObj.getObjectID())) {
	        dbObj.setObjectID(Utils.base64FromBinary(IDGenerator.nextID()));
	    }
	    String tableName = dbObj.getTableName();
	    Utils.require(!Utils.isEmpty(tableName), "Object is missing '_table' definition");
	    TableDefinition tableDef = application.getTableDef(tableName);
	    Utils.require(tableDef != null, "Unknown table for application '%s': %s", application.getAppName(), tableName);
	    TableBuilder b = getTable(tableDef);
	    Doc doc = b.addDoc(dbObj.getObjectID());
	    if(dbObj.isDeleted()) doc.deleted = true;
	    for(FieldDefinition field : tableDef.getFieldDefinitions()) {
	        add(b, doc, field, dbObj.getFieldValues(field.getName()));
	    }
	}
	
	private void add(TableBuilder b, Doc doc, FieldDefinition field, List<String> f) {
	    if(f == null || f.size() == 0) return;
	    switch(field.getType()) {
	    case BOOLEAN:
			for(String fv: f) {
		        boolean bvalue = "true".equalsIgnoreCase(fv);
		        b.addNum(doc, field.getName(), bvalue ? 1 : 0);
			}
	        break;
	    case INTEGER:
	        try {
				for(String fv: f) {
					b.addNum(doc, field.getName(), Integer.parseInt(fv));
				}
	        } catch (NumberFormatException e) {
	            throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
	        }
	        break;
	    case LONG:
	        try {
				for(String fv: f) {
					b.addNum(doc, field.getName(), Long.parseLong(fv));
				}
	        } catch (NumberFormatException e) {
	            throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
	        }
	        break;
        case DOUBLE:
            try {
    			for(String fv: f) {
	                double val = Double.parseDouble(fv);
	                long lval = Double.doubleToRawLongBits(val);
	                b.addNum(doc, field.getName(), lval);
    			}
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
            }
            break;
        case FLOAT:
            try {
    			for(String fv: f) {
	                float val = Float.parseFloat(fv);
	                int ival = Float.floatToRawIntBits(val);
	                b.addNum(doc, field.getName(), ival);
    			}
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid format for field '" + field.getName() + "': " + f.get(0), e);
            }
            break;
	    case LINK:
	        TableBuilder b2 = getTable(field.getInverseTableDef());
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
			for(String fv: f) {
		        Date dvalue = Utils.dateFromString(fv);
		        b.addNum(doc, field.getName(), dvalue.getTime());
			}
	        break;
	    default: throw new IllegalArgumentException("Unknown Olap type " + field.getType().toString());
	    }
	}
	
}
