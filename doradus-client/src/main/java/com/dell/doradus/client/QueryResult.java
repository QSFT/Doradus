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

package com.dell.doradus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Holds the results of an object query. A QueryResult has a basic status (success/failed).
 * If the query failed, it holds an error message. If it succeeded, it holds a list of
 * zero or more "outer" {@link DBObject}s returned by the query. Each DBObject will
 * minimally have an _ID, and it may have scalar and/or link field values. When an object
 * has a link field for which one or fields were returned for linked objects, the linked
 * object and any fields returned for it can be obtained via
 * {@link #getLinkedObject(String, String, String)}. 
 */
public class QueryResult {
    // Immutable members of a query result:
    private final TableDefinition m_tableDef;
    private final boolean         m_bFailed;
    private final String          m_errMsg;
    private final List<DBObject>  m_outerObjectList = new ArrayList<DBObject>();
    
    // A query result may or may not have a continuation token
    private String m_contToken;
    
    // Secondary objects are linked to their owning object by the owning object ID and the
    // link field's name. This means the same object can appear in this map multiple times
    // linked to different owning objects and/or via different links, and each occurrence
    // can different fields depending on what the quey returned. So:
    //      {<owning object ID> : {<link field name>: {linked object ID: <linked object>}}}
    private final Map<String, Map<String, Map<String, DBObject>>> m_linkedObjectMap =
        new HashMap<String, Map<String, Map<String, DBObject>>>();
    
    /**
     * Create a QueryResult that represents a failed query for the given table, using the
     * given error message.
     * 
     * @param tableDef  {@link TableDefinition} of perspective query table.
     * @param errMsg    Text of error message.
     */
    public QueryResult(TableDefinition tableDef, String errMsg) {
        m_tableDef = tableDef;
        m_bFailed = true;
        m_errMsg = errMsg;
    }   // constructor

    /**
     * Create a QueryResult that represents a successful query against the given table,
     * containing the results rooted at the given UNode.
     * 
     * @param tableDef  {@link TableDefinition} of perspective query table.
     * @param rootNode  Root {@link UNode} of a UNode true parsed from a query result.
     */
    public QueryResult(TableDefinition tableDef, UNode rootNode) {
        m_tableDef = tableDef;
        m_bFailed = false;
        m_errMsg = "";
        
        // Query result outer object should be "results".
        Utils.require(rootNode.getName().equals("results"),
                      "Root node should be 'results': " + rootNode.getName());

        // Parse children of "nodes"
        for (UNode childNode : rootNode.getMemberList()) {
            // Root node should have a "docs" array.
            if (childNode.getName().equals("docs")) {
                Utils.require(childNode.isCollection(),  // might be a map if it has only one child node
                              "'docs' node should be an array: " + childNode);
                for (UNode docNode : childNode.getMemberList()) {
                    // Each member should be a 'doc' object.
                    Utils.require(docNode.getName().equals("doc"),
                                  "Children of 'docs' node should be 'doc': " + docNode.getName());
                    
                    // Parse the array elements into a DBObject and add to the outer result lsit.
                    m_outerObjectList.add(parseObject(m_tableDef, docNode));
                }
            } else if (childNode.getName().equals("continue")) {
                Utils.require(childNode.isValue(), "'continue' node should be a value: " + childNode);
                m_contToken = childNode.getValue();
            }
        }
    }   // constructor

    /**
     * Indicate if this QueryResult represents a failed query.
     * 
     * @return  True if this QueryResult represents a failed query.
     */
    public boolean isFailed() {
        return m_bFailed;
    }   // isFailed
    
    /**
     * If this QueryResult represents a failed query, get the failure error message.
     * 
     * @return  The error message of a failed query, otherwise an empty string.
     */
    public String getErrorMessage() {
        return m_errMsg;
    }   // getErrorMessage
    
    /**
     * Get the result objects for this query, which consists of a collection of
     * {@link DBObject}s created from the query's outer "doc" objects. Only the _ID field
     * and the scalar and link fields returned by the query will be filled-in for each
     * object.
     * <p>
     * If an object includes link field values, the corresponding link fields will be
     * populated with the object IDs of the related objects returned by the query. If any
     * additional fields were returned for the linked object, a {@link DBObject} is
     * created to hold these fields, and the object is stored in the internal "linked
     * object cache". A linked object can be retrieved by calling
     * {@link #getLinkedObject(String, String, String)}.
     * 
     * @return  Collection of result {@link DBObject}s returned by this query result. The
     *          collection may be empty but not null.
     */
    public Collection<DBObject> getResultObjects() {
        return m_outerObjectList;
    }   // getResultObjects
    
    /**
     * Get the {@link DBObject} that is linked to the given "owner" object ID via the given
     * link name with the given "linked" object ID. If the requested object was returned by
     * the query, the DBObject minimally contains an object ID but only the additional
     * scalar and/or link fields requested by the query. The same object may be linked to
     * multiple owning objects with different fields, as directed by the query. 
     * 
     * @param owningObjID   Object ID of the object that owns the given link field.
     * @param linkFieldName Name of link field.
     * @param linkedObjID   Object ID of the linked object to fetch.
     * @return              The {@link DBObject} of the linked object if it was returned by
     *                      query, otherwise null.
     */
    public DBObject getLinkedObject(String owningObjID,
                                    String linkFieldName,
                                    String linkedObjID) {
        Map<String, Map<String, DBObject>> objMap = m_linkedObjectMap.get(owningObjID);
        if (objMap != null) {
            Map<String, DBObject> linkMap = objMap.get(linkFieldName);
            if (linkMap != null) {
                return linkMap.get(linkedObjID);
            }
        }
        return null;
    }   // getLinkedObject
    
    /**
     * Get the continutation token for this query result, if any. A query will have a
     * continuation token if a size-limited query is performed and more results are
     * available.
     * 
     * @return This query results continuation token, if any, otherwise null.
     */
    public String getContinuationToken() {
        return m_contToken;
    }   // getContinuationToken
    
    /**
     * Return the {@link TableDefinition} of the perspective table for this query.
     * 
     * @return  The table for which this query was performed.
     */
    public TableDefinition getTableDef() {
        return m_tableDef;
    }   // getTableDef

    ///// Private parsing methods for API version > 1
    
    // Parse the given "doc" UNode, which contains the field name/value pairs for an object
    // returned in a query result, and return a DBObject from those fields. The given
    // UNode come from one of two places. Using JSON as the example:
    // 
    //      {"doc": {
    //          "_ID": "987",
    //          "Name": "Bob",
    //          "Children": [
    //              {"doc": {
    //                  "_ID": "123",
    //                  "Age": "25",
    //                  "Name": "John"
    //              }},
    //              {"doc": {
    //                  "_ID": "456",
    //                  "Age": "23",
    //                  "Name": "Anne"
    //              }},
    //              ...
    //          ]
    //      }}
    // 
    // This method can be called for either the outer "doc" object or an inner "doc"
    // object that is an array value of a link. Note that in the latter case, the
    // TableDefinition passed is for the extent table of the link, which may be different
    // than the perspective table of the query.
    private DBObject parseObject(TableDefinition tableDef, UNode docNode) {
        assert tableDef != null;
        assert docNode != null;
        assert docNode.getName().equals("doc");
        
        // Create the DBObject that we will return and parse the child nodes into it.
        DBObject dbObj = new DBObject();
        for (UNode childNode: docNode.getMemberList()) {
            // Extract field name and characterize what we expect.
            String fieldName = childNode.getName();
            FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
            boolean isLinkField = fieldDef == null ? false : fieldDef.isLinkField();
            boolean isGroupField = fieldDef == null ? false : fieldDef.isGroupField();
            boolean isCollection = fieldDef == null ? false : fieldDef.isCollection();
            Utils.require(!isGroupField,    // we currently don't expect group fields in query results
                          "Unexpected group field in query results: " + fieldName);
            
            // Parse value based on what we expect.
            if (isLinkField) {
                parseLinkValue(dbObj, childNode, fieldDef);
            } else if (isCollection) {
                parseMVScalarValue(dbObj, childNode, fieldDef);
            } else {
                // Simple SV scalar value.
                Utils.require(childNode.isValue(),
                              "Value of an SV scalar must be a value: " + childNode);
                dbObj.addFieldValue(fieldName, childNode.getValue());
            }
        }
        
        // Our object is complete
        return dbObj;
    }   // parseObject
    
    // Parse the link field value rooted at the given UNode and add the values to the
    // given owning DBObject for the given link field. Link values use a JSON format
    // equivalent to the following:
    //
    //      {"Children": [
    //          {"doc": {
    //              "_ID": "123",
    //              "Age": "25",
    //              "Name": "John"
    //          }},
    //          {"doc": {
    //             ...
    //          }},
    //          ...
    //      ]}
    //
    // Minimally, the _ID value is included. We create a DBObject for each inner "doc"
    // object and add it to the linked object cache for the owning object and link.
    private void parseLinkValue(DBObject        owningObj,
                                UNode           linkNode,
                                FieldDefinition linkFieldDef) {
        // Prerequisites:
        assert owningObj != null;
        assert linkNode != null;
        assert linkFieldDef != null;
        assert linkFieldDef.isLinkField();
        TableDefinition tableDef = linkFieldDef.getTableDef();
        
        // Value should be an array, though it could be a map with one child.
        Utils.require(linkNode.isCollection(), "Value of link field should be a collection: " + linkNode);
        
        // Iterate through child nodes.
        TableDefinition extentTableDef =
            tableDef.getAppDef().getTableDef(linkFieldDef.getLinkExtent());
        for (UNode childNode : linkNode.getMemberList()) {
            // Ensure this element is "doc" node.
            Utils.require(childNode.getName().equals("doc"),
                          "link field array values should be 'doc' objects: " + childNode);
            
            // Recurse and build a DBObject from the doc node.
            DBObject linkedObject = parseObject(extentTableDef, childNode);
            
            // Add the linked object to the cache and add its object ID to the set.
            String objID = linkedObject.getObjectID();
            cacheLinkedObject(owningObj.getObjectID(), linkFieldDef.getName(), linkedObject);
            owningObj.addFieldValues(linkFieldDef.getName(), Arrays.asList(objID));
        }
    }   // parseLinkValue

    // Parse the MV scalar field value rooted at the given UNode, which should be an array,
    // and add the value set to the given DBObject as a value of the given collection
    // field.
    private void parseMVScalarValue(DBObject        dbObj,
                                    UNode           scalarNode,
                                    FieldDefinition fieldDef) {
        // Prerequisites:
        assert dbObj != null;
        assert scalarNode != null;
        assert fieldDef != null;
        assert fieldDef.isCollection();
        
        // Value should be an array.
        Utils.require(scalarNode.isCollection(),    // could be a map with no values
                      "Value of MV scalar field should be an array: " + scalarNode);
        
        // Extract values, which should be simple scalar values, and collect into a list.
        List<String> valueList = new ArrayList<String>();
        for (UNode childNode : scalarNode.getMemberList()) {
            Utils.require(childNode.isValue(),
                          "Element of an MV scalar should be a value: " + childNode);
            valueList.add(childNode.getValue());
        }
        
        // Add completed value list as a collection value.
        dbObj.addFieldValues(fieldDef.getName(), valueList);
    }   // parseMVScalarValue

    // Add the given object to the linked object cache for the given owning object ID and
    // link field name.
    private void cacheLinkedObject(String owningObjID,
                                   String linkFieldName,
                                   DBObject linkedObject) {
        // Find or create map for the owning object.
        Map<String, Map<String, DBObject>> objMap = m_linkedObjectMap.get(owningObjID);
        if (objMap == null) {
            objMap = new HashMap<String, Map<String, DBObject>>();
            m_linkedObjectMap.put(owningObjID, objMap);
        }
        
        // Find or create map for the link field.
        Map<String, DBObject> linkMap = objMap.get(linkFieldName);
        if (linkMap == null) {
            linkMap = new HashMap<String, DBObject>();
            objMap.put(linkFieldName, linkMap);
        }
        
        // Add the object to the link map.
        linkMap.put(linkedObject.getObjectID(), linkedObject);
    }   // cacheLinkedObject

}   // class QueryResult
