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

package com.dell.doradus.olap.store;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.olap.XType;
import com.dell.doradus.olap.io.VDirectory;

public class SegmentStats {
	public long totalStoreSize;
	public Map<String, Table> tables = new HashMap<String, Table>();
	
	public Table getTable(String table) {
		return tables.get(table);
	}
	
	public Table.NumField getNumField(String table, String field) {
		Table t = tables.get(table);
		return t == null ? null : t.numFields.get(field);
	}
	public Table.TextField getTextField(String table, String field) {
		Table t = tables.get(table);
		return t == null ? null : t.textFields.get(field);
	}
	public Table.LinkField getLinkField(String table, String field) {
		Table t = tables.get(table);
		return t == null ? null : t.linkFields.get(field);
	}
	
	public void addTable(String name, int documents) {
		Table t = new Table();
		t.name = name;
		t.documents = documents;
		tables.put(name, t);
	}

	public void addNumField(FieldDefinition fieldDef, NumWriter writer) {
		Table t = tables.get(fieldDef.getTableName());
		Table.NumField field = t.new NumField();
		field.name = fieldDef.getName();
		field.type = fieldDef.getType().toString();
		field.min = writer.min;
		field.max = writer.max;
		field.min_pos = writer.min_pos;
		field.bits = writer.bits;
		field.isSingleValued = true;
		t.numFields.put(field.name, field);
	}

	public void addNumField(FieldDefinition fieldDef, NumWriterMV writer) {
		Table t = tables.get(fieldDef.getTableName());
		Table.NumField field = t.new NumField();
		field.name = fieldDef.getName();
		field.type = fieldDef.getType().toString();
		field.min = writer.min;
		field.max = writer.max;
		field.min_pos = writer.min_pos;
		field.bits = writer.bits;
		field.doclistSize = writer.getDocListSize();
		field.isSingleValued = writer.isSingleValued();
		t.numFields.put(field.name, field);
	}
	
	public void addTextField(FieldDefinition fieldDef, FieldWriter writer) {
		Table t = tables.get(fieldDef.getTableName());
		Table.TextField field = t.new TextField();
		field.name = fieldDef.getName();
		field.valuesCount = writer.getValuesCount();
		field.doclistSize = writer.getEntriesCount();
		field.isSingleValued = writer.isSingleValued();
		t.textFields.put(field.name, field);
	}

	public void addTextField(FieldDefinition fieldDef, FieldWriterSV writer) {
		Table t = tables.get(fieldDef.getTableName());
		Table.TextField field = t.new TextField();
		field.name = fieldDef.getName();
		field.valuesCount = writer.getValuesCount();
		field.doclistSize = writer.getValuesCount();
		field.isSingleValued = true;
		t.textFields.put(field.name, field);
	}
	
	public void addLinkField(FieldDefinition fieldDef, FieldWriter writer) {
		Table t = tables.get(fieldDef.getTableName());
		Table.LinkField field = t.new LinkField();
		field.name = fieldDef.getName();
		field.linkedTableName = fieldDef.getLinkExtent();
		field.inverseLink = fieldDef.getLinkInverse();
		field.doclistSize = writer.getEntriesCount();
		field.isSingleValued = writer.isSingleValued();
		t.linkFields.put(field.name, field);
	}

    public void addInverseLinkField(FieldDefinition fieldDef, int fields) {
        Table t = tables.get(fieldDef.getTableName());
        Table.LinkField field = t.new LinkField();
        field.name = fieldDef.getName();
        field.linkedTableName = fieldDef.getLinkExtent();
        field.inverseLink = fieldDef.getLinkInverse();
        field.doclistSize = fields;
        field.isSingleValued = false;
        t.linkFields.put(field.name, field);
    }
	
	public long memory() {
		long mem = 0;
		for(Table t : tables.values()) mem += t.memory();
		return mem;
	}
	
	public int totalObjects() {
		int docs = 0;
		for(Table t : tables.values()) docs += t.documents;
		return docs;
	}
	
	@Override public String toString() {
		return toUNode().toString(ContentType.TEXT_XML);
	};
	
	public void save(VDirectory dir) {
		dir.putProperty("stats.xml", toUNode().toString(ContentType.TEXT_XML));
	}
	
	public static SegmentStats load(VDirectory dir) {
		String stats = dir.getProperty("stats.xml");
		if(stats == null) return new SegmentStats();
        return SegmentStats.fromDoc(UNode.parseXML(stats));
	}

	public UNode toUNode() {
	    UNode stats = UNode.createMapNode("stats");
	    stats.addValueNode("memory", XType.sizeToString(memory()), true);
        stats.addValueNode("storage", XType.sizeToString(totalStoreSize), true);
        stats.addValueNode("documents", XType.toString(totalObjects()), true);
        UNode tablesNode = stats.addMapNode("tables");
        for(Table table : tables.values()) {
            UNode tableNode = tablesNode.addMapNode(table.name, "table");
            tableNode.addValueNode("documents", XType.toString(table.documents), true);
            tableNode.addValueNode("memory", XType.sizeToString(table.memory()), true);
            UNode numFieldsNode = tableNode.addMapNode("numFields");
            for(Table.NumField field : table.numFields.values()) {
                UNode child = numFieldsNode.addMapNode(field.name, "field");
                child.addValueNode("type", field.type, true);
                child.addValueNode("min", XType.toString(field.min), true);
                child.addValueNode("max", XType.toString(field.max), true);
                child.addValueNode("min_pos", XType.toString(field.min_pos), true);
                child.addValueNode("bits", XType.toString(field.bits), true);
                child.addValueNode("doclistSize", XType.toString(field.doclistSize), true);
                child.addValueNode("isSingleValued", XType.toString(field.isSingleValued), true);
                child.addValueNode("memory", XType.sizeToString(field.memory()), true);
            }
            UNode textFieldsNode = tableNode.addMapNode("textFields");
            for(Table.TextField field : table.textFields.values()) {
                UNode child = textFieldsNode.addMapNode(field.name, "field");
                child.addValueNode("valuesCount", XType.toString(field.valuesCount), true);
                child.addValueNode("doclistSize", XType.toString(field.doclistSize), true);
                child.addValueNode("isSingleValued", XType.toString(field.isSingleValued), true);
                child.addValueNode("memory", XType.sizeToString(field.memory()), true);
            }
            UNode linkFieldsNode = tableNode.addMapNode("linkFields");
            for(Table.LinkField field : table.linkFields.values()) {
                UNode child = linkFieldsNode.addMapNode(field.name, "field");
                child.addValueNode("linkedTableName", field.linkedTableName, true);
                child.addValueNode("inverseLink", field.inverseLink, true);
                child.addValueNode("doclistSize", XType.toString(field.doclistSize), true);
                child.addValueNode("isSingleValued", XType.toString(field.isSingleValued), true);
                child.addValueNode("memory", XType.sizeToString(field.memory()), true);
            }
        }
	    return stats;
	}
	
	public static SegmentStats fromDoc(UNode rootNode) {
	    SegmentStats stats = new SegmentStats();
	    stats.totalStoreSize = XType.stringToSize(rootNode.getMemberValue("storage"));
	    UNode tablesNode = rootNode.getMember("tables");
	    for(UNode tableNode : tablesNode.getMemberList()) {
	        Table table = stats.new Table();
	        table.name = tableNode.getName();
	        table.documents = XType.getInt(tableNode.getMemberValue("documents"));
	        UNode numFieldsNode = tableNode.getMember("numFields");
	        if (numFieldsNode != null && numFieldsNode.isCollection()) {
	            for(UNode childNode : numFieldsNode.getMemberList()) {
	                Table.NumField field = table.new NumField();
	                field.name = childNode.getName();
	                field.type = childNode.getMemberValue("type");
	                field.min = XType.getLong(childNode.getMemberValue("min"));
	                field.max = XType.getLong(childNode.getMemberValue("max"));
	                field.min_pos = XType.getLong(childNode.getMemberValue("min_pos"));
	                field.bits = XType.getInt(childNode.getMemberValue("bits"));
	                Integer doclistsize = XType.getInt(childNode.getMemberValue("doclistSize"));
                    field.doclistSize = doclistsize == null ? 0 : doclistsize;
                    Boolean issinglevalued = XType.getBoolean(childNode.getMemberValue("isSingleValued"));
                    field.isSingleValued = issinglevalued == null ? true : issinglevalued;
	                table.numFields.put(field.name, field);
	            }
	        }
            UNode textFieldsNode = tableNode.getMember("textFields");
            if (textFieldsNode != null && textFieldsNode.isCollection()) {
                for(UNode childNode : textFieldsNode.getMemberList()) {
                    Table.TextField field = table.new TextField();
                    field.name = childNode.getName();
                    field.valuesCount = XType.getInt(childNode.getMemberValue("valuesCount"));
                    field.doclistSize = XType.getInt(childNode.getMemberValue("doclistSize"));
                    field.isSingleValued = XType.getBoolean(childNode.getMemberValue("isSingleValued"));
                    table.textFields.put(field.name, field);
                }
            }
            UNode linkFieldsNode = tableNode.getMember("linkFields");
            if (linkFieldsNode != null && linkFieldsNode.isCollection()) {
                for(UNode childNode : linkFieldsNode.getMemberList()) {
                    Table.LinkField field = table.new LinkField();
                    field.name = childNode.getName();
                    field.linkedTableName = childNode.getMemberValue("linkedTableName");
                    field.inverseLink = childNode.getMemberValue("inverseLink");
                    field.doclistSize = XType.getInt(childNode.getMemberValue("doclistSize"));
                    field.isSingleValued = XType.getBoolean(childNode.getMemberValue("isSingleValued"));
                    table.linkFields.put(field.name, field);
                }
            }
	        stats.tables.put(table.name, table);
	    }
	    return stats;
	}
	
	public class Table {
		public String name;
		public int documents;
		public Map<String, NumField> numFields = new HashMap<String, NumField>();
		public Map<String, TextField> textFields = new HashMap<String, TextField>();
		public Map<String, LinkField> linkFields = new HashMap<String, LinkField>();
		
		public Table() { }
		
		public long memory() {
			long mem = 0;
			for(NumField field : numFields.values()) mem += field.memory();
			for(TextField field : textFields.values()) mem += field.memory();
			for(LinkField field : linkFields.values()) mem += field.memory();
			return mem;
		}
		
		public class NumField {
			public String name;
			public String type;
			public long min;
			public long max;
			public long min_pos;
			public int bits;
			public int doclistSize;
			public boolean isSingleValued;
			
			public NumField() { }
			public NumField(String name) { this.name = name; }
			
			public String table() { return Table.this.name; }
			public int documents() { return Table.this.documents; }
			public long memory() {
				if(isSingleValued) return documents() * bits / 8 + documents() / 8;
				else return documents() * 4 + doclistSize * bits / 8;
			}
		}

		public class TextField {
			public String name;
			public int valuesCount;
			public int doclistSize;
			public boolean isSingleValued;
			
			public TextField() { }
			public TextField(String name) { this.name = name; }
			
			public String table() { return Table.this.name; }
			public int documents() { return Table.this.documents; }
			
			public long memory() {
				long mem = documents() * 4;
				if(!isSingleValued) mem += doclistSize * 4;
				return mem;
			}
		}
		
		public class LinkField {
			public String name;
			public String linkedTableName; 
			public String inverseLink;
			public int doclistSize;
			public boolean isSingleValued;
			
			public LinkField() { }
			public LinkField(String name) { this.name = name; }
			
			public String table() { return Table.this.name; }
			public int documents() { return Table.this.documents; }
			public int linkedDocuments() { return SegmentStats.this.getLinkField(linkedTableName, inverseLink).documents(); }
			
			public long memory() {
				long mem = documents() * 4;
				if(!isSingleValued) mem += doclistSize * 4;
				return mem;
			} 
			
		}
		
	}
}
