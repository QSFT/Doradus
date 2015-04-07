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

package com.dell.doradus.olap.builder;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.JSONAnnie;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapDocument;

public class BatchBuilder {
	// SajListener to parse JSON directly into OlapBatch/OlapDocument objects. Example JSON
	// structure we expect:
    //    {"batch": {
    //        "docs": [
    //           {"doc": {
    //              "_ID": "sassafras",
    //              "_table": "Books",
    //              "ISBN": "978-0061673733",
    //              "Address": {
    //                 "City": "Aliso Viejo",
    //                 "State": "CA",
    //                 "Zipcode": 92656
    //              },
    //              "Children": {"add": [123, 456]},
    //              "Tags": {"add": ["Biography", "Philosophy"]}
    //           }},
    //           {"doc": {
    //              ...
    //           }}
    //        ]
    //     }}
	//
	//
	// update Nov. 19, 2013: to delete object, specify
    //    {"batch": {
    //        "docs": [
    //           {"doc": {
    //              "_ID": "sassafras",
    //              "_table": "Books",
    //              "_deleted": "true"
    //           }},
    //           {"doc": {
    //              ...
    //           }}
    //        ]
    //     }}
	//
	static class Listener implements JSONAnnie.SajListener {
	    OlapBatch result = new OlapBatch();
        // Name keys use dotted notation; e.g. _ID=[sassafras], Address.City=[Aliso Viejo],
        Map<String, List<String>> valueMap = new HashMap<>();
        List<String> fieldStack = new ArrayList<>();
	    Set<String> values = new HashSet<String>();
        StringBuilder buffer = new StringBuilder();
	    int level = 0;   // 0=batch object, 1=docs array, 2=doc object, 3+=field object

        @Override
        public void onStartObject(String name) {
            switch (level) {
            case 0:     // outer batch level 
                Utils.require(name.equals("batch"), "Root node must be 'batch': " + name);
                break;
            case 1:     // docs level: can be an object with one "doc" child
                Utils.require(name.equals("docs"), "'docs' array expected: " + name);
                break;
            case 2:     // doc object
                Utils.require(name.equals("doc"), "'doc' object expected: " + name);
                valueMap.clear();
                fieldStack.clear();
                break;
            default:     // outer or nested field
                fieldStack.add(name);
            }
            level++;
        }   // onStartObject

        @Override
        public void onEndObject() {
            if (fieldStack.size() > 0) {
                fieldStack.remove(fieldStack.size() - 1);
            }
            if (--level == 2) {
                buildObject();    // just finished a "doc" object
            }
        }   // onEndObject

        @Override
        public void onStartArray(String name) {
            switch (level) {
            case 0:
            case 2:     // An array is unexpected for "batch" or "doc"
                Utils.require(false, "Unexpected array start: " + name);
            case 1:     // Should be "docs" array.
                Utils.require(name.equals("docs"), "'docs' array expected: " + name);
                break;
            default:    // outer or nested field.
                fieldStack.add(name);
            }
            level++;
        }   // onStartArray

        @Override
        public void onEndArray() {
            if (fieldStack.size() > 0) {
                fieldStack.remove(fieldStack.size() - 1);
            }
            level--;
        }   // onEndArray

        @Override
        public void onValue(String name, String value) {
            // Values are only expected within field elements (level >= 3)
            Utils.require(level >= 3, "Unexpected recognized element: %s", name);
            saveValue(name, value);
        }   // onValue
        
        // Save a leaf-level value
        private void saveValue(String name, String value) {
            String dottedName = getDottedName(name);
            List<String> values = valueMap.get(dottedName);
            if (values == null) {
                values = new ArrayList<String>(1);
                valueMap.put(dottedName, values);
            }
            values.add(value);
        }   // saveValue
        
        // Turn parent node names, if any, into a dotted string and append name.
        private String getDottedName(String name) {
            buffer.setLength(0);
            for (String parentName : fieldStack) {
                buffer.append(parentName);
                buffer.append(".");
            }
            buffer.append(name);
            return buffer.toString();
        }   // getDottedName
        
        // Create a DBObject for the just-parsed 'doc' element and add to batch.
        private void buildObject() {
            OlapDocument document = result.addDoc();
            for (String dottedName : valueMap.keySet()) {
                List<String> values = valueMap.get(dottedName);
                String[] names = dottedName.split("\\.");
                if (names.length == 1) {
                    addValues(document, names[0], values);
                } else {
                    addValue(document, names, 0, values);
                }
            }
        }   // buildObject
        
        // Possible name structures we expect:
        //      field.add.value or field.remove.value
        //      group.field
        //      group.field.add.value
        //      group1.group2.field
        //      group1.group2.field.add.value
        //      ...
        private void addValue(OlapDocument document, String[] names, int inx, List<String> values) {
            String fieldName = names[inx];
            if ((names.length - inx) == 3 &&
                 names[inx + 2].equals("value") &&
                 names[inx + 1].equals("add")) {
                // field.add.value
                addValues(document, fieldName, values);
            } else if ((names.length - inx) > 1) {
                // group.field...: recurse to inx + 1.
                addValue(document, names, inx + 1, values);
            } else {
                // field
                addValues(document, fieldName, values);
            }
        }   // addValue
        
        private void addValues(OlapDocument document, String fieldName, Iterable<String> values) {
            for (String value : values) {
                addValue(document, fieldName, value);
            }
        }   // addValues
        
        private void addValue(OlapDocument document, String fieldName, String value) {
            if (Utils.isEmpty(value)) return;
            if (fieldName.equals("_ID")) {
                document.setId(value);
            } else if (fieldName.equals("_table")) {
                document.setTable(value);
            } else if (fieldName.equals("_deleted")) {
                document.setDeleted("true".equals(value));
            } else {
            	document.addField(fieldName, value);
            }
        }   // addValue
    }   // class Listener
	
	// Uses SajListener to parse text directly into an OlapBatch
	public static OlapBatch parseJSON(String text) {
	    Listener listener = new Listener();
	    new JSONAnnie(text).parse(listener);
	    return listener.result;
	}
	
	// Uses SajListener to parse data from a Reader into an OlapBatch 
	public static OlapBatch parseJSON(Reader reader) {
	    Listener listener = new Listener();
	    new JSONAnnie(reader).parse(listener);
	    return listener.result;
	}
	
    public static OlapBatch fromUNode(UNode rootNode) {
        Utils.require(rootNode.getName().equals("batch"),
                      "Root node must be 'batch': " + rootNode.getName());
        OlapBatch batch = new OlapBatch();
        UNode docsNode = rootNode.getMember("docs");
        Utils.require(docsNode != null, "'batch' node requires child 'docs' node");
        for (UNode docNode : docsNode.getMemberList()) {
            // Get "doc" node and its _ID and _table values.
            Utils.require(docNode.getName().equals("doc"), "'doc' node expected: " + docNode.getName());
            OlapDocument document = batch.addDoc();
            for (UNode fieldNode : docNode.getMemberList()) {
                addFieldValues(document, fieldNode);
            }
            Utils.require(document.getTable() != null, "'doc' node missing '_table' value");
        }
        return batch;
    }   // fromUNode

    private static void addFieldValues(OlapDocument document, UNode fieldNode) {
        String fieldName = fieldNode.getName();
        Utils.require(fieldName != null, "'field' node missing 'name' value:" + fieldNode.getName());
        if (fieldNode.isValue()) {
            // Simple field value
            addFieldValue(document, fieldName, fieldNode.getValue());
        } else if (fieldNode.isArray()) {
            // ["value 1", "value 2", ...]
            for (UNode valueNode : fieldNode.getMemberList()) {
                Utils.require(valueNode.isValue(), "Value expected: " + valueNode);
                addFieldValue(document, fieldName, valueNode.getValue());
            }
        } else {
            // Field's value is MAP.
            for (UNode childNode : fieldNode.getMemberList()) {
                if (childNode.getName().equals("add") && !childNode.isValue()) {
                    // "add": ["value 1", "value 2", ...]
                    for (UNode valueNode : childNode.getMemberList()) {
                        Utils.require(valueNode.isValue(), "Value expected: " + valueNode);
                        addFieldValue(document, fieldName, valueNode.getValue());
                    }
                } else {
                    // Must be a nested field.
                    addFieldValues(document, childNode);
                }
            }
        }
    }   // addFieldValues

    private static void addFieldValue(OlapDocument document, String fieldName, String value) {
        if (fieldName.equals("_ID")) {
            document.setId(value);
        } else if (fieldName.equals("_table")) {
            document.setTable(value);
        } else if (fieldName.equals("_deleted")) {
            document.setDeleted("true".equals(value));
        } else {
            document.addField(fieldName, value);
        }
    }   // addFieldValue

}
