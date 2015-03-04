package com.dell.doradus.olap.builder2;

import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.JSONAnnie;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

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
	    OlapBatch2 result = new OlapBatch2();
	    OlapDocument2 document;
	    String field;
	    Set<String> values = new HashSet<String>();
	    int level = 0;   // 0=batch object, 1=docs array, 2=doc object, 3+=field object
	    boolean bInArray = false;

        @Override
        public void onStartObject(String name) {
            switch (level) {
            case 0:     // outer batch level 
                Utils.require(name.equals("batch"), "Root node must be 'batch': " + name);
                level++;
                break;
            case 1:     // docs level: should have been array
                Utils.require(false, "'docs' array expected: " + name);
                break;
            case 2:     // doc object
                Utils.require(name.equals("doc"), "'doc' object expected: " + name);
                document = result.addDoc(); 
                level++;
                break;
            default:     // outer or nested field
                field = name;
                level++;
                break;
            }
        }   // onStartObject

        @Override
        public void onEndObject() {
            level--;
        }   // onEndObject

        @Override
        public void onStartArray(String name) {
            if (level == 1) {
                // Should be "docs" array.
                Utils.require(name.equals("docs"), "'docs' array expected: " + name);
                level++;
            } else if (level >= 3) {
                // Must be "add" node for MV field.
                Utils.require(name.equals("add"), "Unrecognized array start: " + name);
                values.clear();
                level++;
                bInArray = true;
            } else {
                // Level is 0 (batch) or 2 (doc), where an array is unexpected
                Utils.require(false, "Unexpected array start: " + name);
            }
        }   // onStartArray

        @Override
        public void onEndArray() {
            level--;
            bInArray = false;
            if (level >= 3) {
                // Just finished "add" array for an MV field.
                for (String value : values) {
                    addValue(field, value);
                }
            }
        }   // onEndArray

        @Override
        public void onValue(String name, String value) {
            // Values only expected for fields (level >= 3)
            Utils.require(level >= 3, "Unrecognized element: %s", name);
            
            // Add value to current SV field
            if (bInArray) {
                values.add(value);
            } else {
                addValue(name, value);
            }
        }   // onValue
        
        private void addValue(String fieldName, String value) {
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
	public static OlapBatch2 parseJSON(String text) {
	    Listener listener = new Listener();
	    new JSONAnnie(text).parse(listener);
	    return listener.result;
	}
	
	// Uses SajListener to parse data from a Reader into an OlapBatch 
	public static OlapBatch2 parseJSON(Reader reader) {
	    Listener listener = new Listener();
	    new JSONAnnie(reader).parse(listener);
	    return listener.result;
	}
	
    public static OlapBatch2 fromUNode(UNode rootNode) {
        Utils.require(rootNode.getName().equals("batch"),
                      "Root node must be 'batch': " + rootNode.getName());
        OlapBatch2 batch = new OlapBatch2();
        UNode docsNode = rootNode.getMember("docs");
        Utils.require(docsNode != null, "'batch' node requires child 'docs' node");
        for (UNode docNode : docsNode.getMemberList()) {
            // Get "doc" node and its _ID and _table values.
            Utils.require(docNode.getName().equals("doc"), "'doc' node expected: " + docNode.getName());
            OlapDocument2 document = batch.addDoc();
            for (UNode fieldNode : docNode.getMemberList()) {
                addFieldValues(document, fieldNode);
            }
            Utils.require(document.getTable() != null, "'doc' node missing '_table' value");
        }
        return batch;
    }   // fromUNode

    private static void addFieldValues(OlapDocument2 document, UNode fieldNode) {
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

    private static void addFieldValue(OlapDocument2 document, String fieldName, String value) {
        if (fieldName.equals("_ID")) {
            document.setId(value);
        } else if (fieldName.equals("_table")) {
            document.setId(value);
        } else if (fieldName.equals("_deleted")) {
            document.setDeleted("true".equals(value));
        } else {
            document.addField(fieldName, value);
        }
    }   // addFieldValue

}
