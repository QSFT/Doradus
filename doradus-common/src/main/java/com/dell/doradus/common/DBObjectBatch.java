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

package com.dell.doradus.common;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a batch of {@link DBObject}s being added or updated. If all objects belong
 * to the same table, m_tableDef will be defined. If objects may belong to different
 * tables, m_tableDef will be null, and each DBObject points to the table to which it
 * belongs.
 */
public class DBObjectBatch implements JSONable{
    // Members:
    private final List<DBObject> m_dbObjList = new ArrayList<DBObject>();
    
    // SajListener to parse JSON directly into DBObjectBatch/DBObject objects. Example JSON
    // structure we expect:
    //
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
    //              "_ID": "xyzzy",
    //              "_table": "Books",
    //              "_deleted": "true"
    //           }},
    //           {"doc": {
    //              ...
    //           }}
    //        ]
    //     }}
    // 
    // Since this class is not static, each instance belongs to a DBObjectBatch instance.
    class Listener implements JSONAnnie.SajListener {
        // Name keys use dotted notation; e.g. _ID=[sassafras], Address.City=[Aliso Viejo],
        Map<String, List<String>> valueMap = new HashMap<>();
        List<String> fieldStack = new ArrayList<>();
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
            DBObject dbObj = addObject(null, null);
            for (String dottedName : valueMap.keySet()) {
                List<String> values = valueMap.get(dottedName);
                String[] names = dottedName.split("\\.");
                addObjectValue(dbObj, names, 0, values);
            }
        }   // buildObject
        
        // Possible name structures we expect:
        //      field
        //      field.value
        //      field.add.value or field.remove.value
        //      group.field
        //      group.field.[add|remove].value
        //      group1.group2.field
        //      group1.group2.field.[add|remove].value
        //      ...
        private void addObjectValue(DBObject dbObj, String[] names, int inx, List<String> values) {
            String fieldName = names[inx];
            if ((names.length - inx) == 2 && names[inx + 1].equals("value")) {
                // field.value
                dbObj.addFieldValues(fieldName, values);
            } else if ((names.length - inx) == 3 &&
                       names[inx + 2].equals("value") &&
                       (names[inx + 1].equals("add") || names[inx + 1].equals("remove"))) {
                // field.add.value or field.remove.value
                if (names[inx + 1].equals("add")) {
                    dbObj.addFieldValues(fieldName, values);
                } else {
                    dbObj.removeFieldValues(fieldName, values);
                }
            } else if ((names.length - inx) > 1) {
                // group.field...: recurse to inx + 1.
                addObjectValue(dbObj, names, inx + 1, values);
            } else {
                // field
                dbObj.addFieldValues(fieldName, values);
            }
        }   // addObjectValue
        
    }   // class Listener
    
    /**
     * Construct a new, empty batch object.
     */
    public DBObjectBatch() {
    }   // constructor
    
    /**
     * Parse a batch update described in JSON from a Reader, adding {@link DBObject}s
     * to this batch object for each 'doc' element found.
     * 
     * @param reader    Character reader from which JSON text is to be read. 
     */
    public void parseJSON(Reader reader) {
        Listener sajListener = new Listener();
        new JSONAnnie(reader).parse(sajListener);
    }   // parseJSON
    
    /**
     * Parse a batch object update rooted at the given UNode. The root node must be a MAP
     * named "batch". Child nodes must be a recognized option or a "docs" array containing
     * "doc" objects. An exception is thrown if the batch is malformed.
     * 
     * @param rootNode Root node of a UNode tree defined an object update batch.
     */
    public void parse(UNode rootNode) {
        assert rootNode != null;
        
        // Ensure root node is named "batch".
        Utils.require(rootNode.getName().equals("batch"),
                      "'batch' expected: " + rootNode.getName());

        // Parse child nodes.
        for (String memberName : rootNode.getMemberNames()) {
            UNode childNode = rootNode.getMember(memberName);
            if (childNode.getName().equals("docs")) {
                Utils.require(childNode.isCollection(), "'docs' must be a collection: " + childNode);
                for (UNode docNode : childNode.getMemberList()) {
                    Utils.require(docNode.getName().equals("doc"),
                                  "'doc' node expected as child of 'docs': " + docNode);
                    addObject(new DBObject()).parse(docNode);
                }
            } else {
                Utils.require(false, "Unrecognized child node of 'batch': " + memberName);
            }
        }
    }   // parse

    ////////// Getters
    
    /**
     * Get the collection of {@link DBObject}s contained in this batch. The collection is
     * not copied.
     * 
     * @return  The list of {@link DBObject}s contained in this batch.
     */
    public Iterable<DBObject> getObjects() {
        return m_dbObjList;
    }   // getObjects

    /**
     * Return the number of objects currently contained in this batch.
     * 
     * @return  The number of objects currently contained in this batch.
     */
    public int getObjectCount() {
        return m_dbObjList.size();
    }   // getObjectCount

    /**
     * Serialize this DBObjectBatch object into a UNode tree and return the root node.
     * 
     * @return  Root node of a UNode tree representing this batch update.
     */
    public UNode toDoc() {
        // Root object is a MAP called "batch".
        UNode batchNode = UNode.createMapNode("batch");
        
        // Add a "docs" node as an array.
        UNode docsNode = batchNode.addArrayNode("docs");
        for (DBObject dbObj : m_dbObjList) {
            docsNode.addChildNode(dbObj.toDoc());
        }
        return batchNode;
    }   // toDoc

    ////////// Setters
    
    /**
     * Add a {@link DBObject} to this batch.
     * 
     * @param dbObj     DBObject to add to this batch.
     * @return          Same DBObject.
     */
    public DBObject addObject(DBObject dbObj) {
        m_dbObjList.add(dbObj);
        return dbObj;
    }   // addObject

    /**
     * Create a new DBObject with the given object ID and table name, add it to this
     * DBObjectBatch, and return it.
     * 
     * @param objID     New DBObject's object ID, if any.
     * @param tableName New DBObject's table name, if any.
     * @return          New DBObject.
     */
    public DBObject addObject(String objID, String tableName) {
        DBObject dbObj = new DBObject(objID, tableName);
        m_dbObjList.add(dbObj);
        return dbObj;
    }   // addObject
    
    /**
     * Clear this batch of all {@link DBObject}s, if any.
     */
    public void clear() {
        m_dbObjList.clear();
    }   // clear
    
    @Override
    public String toJSON() {
        return toDoc().toJSON();
    }
    
    ///// Builder 
    
    /**
      * Creates a new {@link Builder} instance. This is a convenience method for
      * {@code new Builder()}.
      *
      * @return New DBObjectBatch {@link Builder}.
      */
     public static DBObjectBatch.Builder builder() {
         return new DBObjectBatch.Builder();
     }
        
     /**
      * Helper class to build {@link DBObjectBatch} instances.
      */
     public static class Builder {
        private DBObjectBatch dbObjectBatch = new DBObjectBatch();
                    
        /**
         * Returns the completed {@link DBObjectBatch}. 
         *
         * @return The newly built DBObjectBatch instance.
         */
        public DBObjectBatch build() {
            return this.dbObjectBatch;
        }

        /**
         * Add the given DBObject to the builder.
         * 
         * @param dbObject  New {@link DBObject}.
         * @return          This {@link Builder}.
         */
        public Builder withObject(DBObject dbObject) {
            dbObjectBatch.addObject(dbObject);
            return this;
        }   
    }

}   // class DBObjectBatch
