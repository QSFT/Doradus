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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Holds field values for a Doradus database object. When a DBObject is created by
 * fetching an object from the database, the _ID values is always present. Additional
 * scalar and/or link field values are present if requested and if a value was found.
 * Field values can be retrieved via these methods:
 * <pre>
 *      {@link #getFieldNames()}
 *      {@link #getFieldValue(String)}      // when field has a single value
 *      {@link #getFieldValues(String)}
 *      {@link #getObjectID()}
 *      {@link #hasFieldValue(String)}
 * </pre>
 * 
 * When used to add a new object, a DBObject holds values to be added for both scalar and
 * link fields. A DBObject does not need an object ID if it is new and should receive a
 * unique, system-generated ID. To update an existing object, an ID must be defined.
 * The ID field and values to be added to a field are assigned via the methods:
 * 
 * <pre>
 *      {@link #addFieldValue(String, String)}
 *      {@link #addFieldValues(String, Collection)}
 *      {@link #setFieldValue(String, String)}
 *      {@link #setObjectID(String)}
 * </pre>
 * 
 * A DBObject that is used to update an existing object can also define values to be
 * removed for existing MV scalar and link fields. Values to be removed from a field are
 * assigned via these methods:
 * 
 * <pre>
 *      {@link #removeFieldValues(String, Collection)}
 *      {@link #setFieldValue(String, String)}
 * </pre>
 * 
 * To determine which fields have been assigned new values, "remove" values, or either,
 * use these methods:
 * 
 * <pre>
 *      {@link #getFieldNames()}
 *      {@link #getRemoveValues(String)}
 *      {@link #getUpdatedFieldNames()}
 *      {@link #getLinkFieldNames(TableDefinition)}
 *      {@link #getScalarFieldNames(TableDefinition)}
 *      {@link #getUpdatedScalarFieldNames(TableDefinition)}
 *      {@link #hasFieldValue(String)}
 *      {@link #hasObjectID()}
 * </pre>
 * 
 * DBObject does not verify field types or cardinality: if a field is assigned value(s)
 * incompatible with its type or cardinality, an error occurs when the update is applied.
 * <p>
 * A DBObject can be serialized into a {@link UNode} tree or created from a parsed UNode
 * tree via these methods:
 * 
 * <pre>
 *      {@link #parse(UNode)}
 *      {@link #toDoc()}
 *      {@link #toGroupedDoc(TableDefinition)}
 * </pre>
 */
final public class DBObject {
    // Members:
    private final Map<String, Set<String>>  m_valueMap = new HashMap<>();
    private final Map<String, Set<String>>  m_valueRemoveMap = new HashMap<>();
    
    /**
     * Create a new, empty DBObject. The object will have no _ID value.
     */
    public DBObject() {
    }   // constructor
    
    ///// Getters

    /**
     * Return the field names for which this DBObject has a value. The set does not
     * include fields that have "remove" values stored. The set is copied.
     * 
     * @return Field names for which this DBObject has a value. Empty if there are no
     *         field values for this object.
     * @see    #getUpdatedFieldNames()
     */
    public Set<String> getFieldNames() {
        return new HashSet<String>(m_valueMap.keySet());
    }   // getFieldNames
    
    /**
     * Return the set of link field names that have values for this object. The given
     * {@link TableDefinition} is used to determine which field names are links.
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Set of link field names that have updates for this object.
     */
    public Set<String> getLinkFieldNames(TableDefinition tableDef) {
        Set<String> nameSet = new HashSet<>();
        for (String fieldName : m_valueMap.keySet()) {
            if (tableDef.isLinkField(fieldName)) {
                nameSet.add(fieldName);
            }
        }
        return nameSet;
    }   // getLinkFieldNames
    
    /**
     * Return the set of scalar field names that have values for this object. The given
     * {@link TableDefinition} is used to determine which field names are scalars.
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Set of scalar field names that have updates for this object.
     */
    public Set<String> getScalarFieldNames(TableDefinition tableDef) {
        Set<String> nameSet = new HashSet<>();
        for (String fieldName : m_valueMap.keySet()) {
            if (tableDef.isScalarField(fieldName)) {
                nameSet.add(fieldName);
            }
        }
        return nameSet;
    }   // getScalarFieldNames

    /**
     * Get all values marked for removal for the given field. The result will be null
     * if the given field has no values to remove. Otherwise, the set is copied.
     * 
     * @param  fieldName    Name of link field.
     * @return              Set of all values to be removed for the given link field, or
     *                      null if there are none.
     */
    public Set<String> getRemoveValues(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        if (!m_valueRemoveMap.containsKey(fieldName)) {
            return null;
        }
        return new HashSet<>(m_valueRemoveMap.get(fieldName));
    }   // getRemoveValues
    
    /**
     * Get this object's ID or null if no object ID has yet been assigned. This is a
     * convenience method that returns the value of the "_ID" field, if set. The _ID
     * field can have only one value.
     * 
     * @return  This DBObject's ID or null if none has been assigned.
     */
    public String getObjectID() {
        Set<String> values = m_valueMap.get(CommonDefs.ID_FIELD);
        if (values != null && values.size() > 0) {
            return values.iterator().next();
        }
        return null;
    }   // getObjectID
    
    /**
     * Get the names of all fields updated by this object, including fields that have new
     * and/or remove values assigned. The set is copied and may be empty but not null.
     * 
     * @return Set of all field names being added or remove for this object.
     */
    public Set<String> getUpdatedFieldNames() {
        Set<String> fieldNames = new HashSet<String>(m_valueMap.keySet());
        fieldNames.addAll(m_valueRemoveMap.keySet());
        return fieldNames;
    }   // getUpdatedFieldNames

    /**
     * Return the names of all scalar fields for which new or "remove" values have been
     * assigned for this object. The given {@link TableDefinition} is used to determine
     * which field names are scalars.
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Set of all updated scalar field names.
     */
    public Set<String> getUpdatedScalarFieldNames(TableDefinition tableDef) {
        Set<String> fieldNames = new HashSet<String>();
        for (String fieldName : m_valueMap.keySet()) {
            if (tableDef.isScalarField(fieldName)) {
                fieldNames.add(fieldName);
            }
        }
        for (String fieldName : m_valueRemoveMap.keySet()) {
            if (tableDef.isScalarField(fieldName)) {
                fieldNames.add(fieldName);
            }
        }
        return fieldNames;
    }   // getUpdatedScalarFieldNames
    
    /**
     * Get the single value of the field with the given name or null if no value is
     * assigned to the given field. This method is intended to be used for fields expected
     * to have a single value. An exception is thrown if it is called for a field with
     * multiple values.
     * 
     * @param fieldName Name of a field.
     * @return          Value of field for this object or null if there is none.
     */
    public String getFieldValue(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        Set<String> values = m_valueMap.get(fieldName);
        if (values == null || values.size() == 0) {
            return null;
        } else {
            Utils.require(values.size() == 1, "Field has more than 1 value: %s", fieldName);
            return values.iterator().next();
        }
    }   // getFieldValue
    
    /**
     * Get all values of the field with the given name or null if no values are assigned
     * to the given field. If the field has at least one value, the set is copied.
     * 
     * @param  fieldName    Name of a field.
     * @return              Set of all values assigned to the field.
     */
    public Set<String> getFieldValues(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        if (!m_valueMap.containsKey(fieldName)) {
            return null;
        }
        return new HashSet<>(m_valueMap.get(fieldName));
    }   // getFieldValues
    
    /**
     * Return true if this object has a value for the field with the given name. This
     * method only indicates fields with new values; a field may have a "remove" value
     * not reflected by this method.
     * 
     * @param fieldName Name of a field.
     * @return          True if this object has a value for the given field name.
     */
    public boolean hasFieldValue(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        return m_valueMap.containsKey(fieldName);
    }   // hasFieldValue
    
    /**
     * Indicate if this object has an object ID. This method returns true if a value
     * for the field named "_ID".
     * 
     * @return True if this object has an object ID.
     */
    public boolean hasObjectID() {
        return m_valueMap.containsKey(CommonDefs.ID_FIELD);
    }   // hasObjectID
    
    /**
     * Serialize this DBObject into a {@link UNode} tree, returning the root node. The
     * tree returned can then be turned into JSON or XML by calling {@link UNode#toJSON()}
     * or {@link UNode#toXML()}. This method ignores the structure of group fields: all
     * fields are returned in a "flat" structure. For example, a UNode tree converted to
     * JSON might look like:
     * <pre>
     *     {"doc": {
     *          "_ID": "28cn2812ur",
     *          "_table": "Message",
     *          "Subject": "Concrete slabs",
     *          "Tags": {
     *              "add": ["Confidential", "Priority"]   // MV values are sorted
     *          },
     *          "Sender": ["1dj2r4j1l"]
     *     }}
     * </pre>
     * 
     * @return The root node of a UNode tree.
     * @see    #toGroupedDoc(TableDefinition).
     */
    public UNode toDoc() {
        UNode resultNode = UNode.createMapNode("doc");
        for (String fieldName : getUpdatedFieldNames()) {
            leafFieldtoDoc(resultNode, fieldName);
        }
        return resultNode;
    }   // toDoc
    
    /**
     * Serialize this DBObject into a {@link UNode} tree, showing nested fields within
     * their parent group fields. The root node of the UNode tree is returned. The tree
     * returned can then be turned into JSON or XML by calling {@link UNode#toJSON()} or
     * {@link UNode#toXML()}. This method uses the given {@link TableDefinition} to
     * understand the table's group field structure. For example, a UNode tree converted
     * to JSON might look like:
     * <pre>
     *     {"doc": {
     *          "_ID": "28cn2812ur",
     *          "_table": "Message",
     *          "Content": {
     *              "Subject": "Concrete slabs",
     *          },
     *          "Tags": {
     *              "add": ["Confidential", "Priority"]   // MV values are sorted
     *          },
     *          "Sender": ["1dj2r4j1l"]
     *     }}
     * </pre>
     * 
     * @return The root node of a UNode tree.
     */
    public UNode toGroupedDoc(TableDefinition tableDef) {
        Utils.require(tableDef != null, "tableDef");
        UNode resultNode = UNode.createMapNode("doc");
        
        Set<FieldDefinition> deferredFields = new HashSet<FieldDefinition>();
        for (String fieldName : getUpdatedFieldNames()) {
            FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
            if (fieldDef != null && fieldDef.isNestedField()) {
                FieldDefinition groupFieldDef = fieldDef.getParentField();
                while (groupFieldDef != null && !deferredFields.contains(groupFieldDef)) {
                    deferredFields.add(groupFieldDef);
                    groupFieldDef = groupFieldDef.getParentField();
                }
                deferredFields.add(fieldDef);
            } else {
                leafFieldtoDoc(resultNode, fieldName);
            }
        }
        
        for (FieldDefinition fieldDef : deferredFields) {
            if (!fieldDef.isNestedField()) {
                groupFieldtoDoc(resultNode, fieldDef, deferredFields);
            }
        }
        return resultNode;
    }   // toGroupedDoc
    
    /**
     * Returns the string "Object: ID" where ID is the object's ID, if defined.
     * 
     * @return  A diagnostic string, e.g., for debugging.
     */
    @Override
    public String toString() {
        return "Object: " + getObjectID();
    }   // toString()

    ///// Setters
    
    /**
     * Add the value(s) in the given collection to the field with the given name. An
     * exception is thrown if the _ID field receives more than one value. If the given
     * collection is null or empty, this method is a no-op.
     * 
     * @param fieldName Name of field.
     * @param values    Collection of values. Ignored if null or empty.
     */
    public void addFieldValues(String fieldName, Collection<String> values) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        if (values != null && values.size() > 0) {
            Set<String> currValues = m_valueMap.get(fieldName);
            if (currValues == null) {
                currValues = new HashSet<>();
                m_valueMap.put(fieldName, currValues);
            }
            currValues.addAll(values);
            Utils.require(!fieldName.equals("_ID") || currValues.size() == 1, "Only 1 value can be set for _ID");
        }
    }   // addFieldValues

    /**
     * Add the given value to the field with the given name. An exception is thrown if
     * the _ID field receives more than one value. If the given value is null or empty,
     * this method is a no-op.
     * 
     * @param fieldName Name of a field.
     * @param value     Value to add to field. Ignored if null or empty.
     */
    public void addFieldValue(String fieldName, String value) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        if (!Utils.isEmpty(value)) {
            Set<String> currValues = m_valueMap.get(fieldName);
            if (currValues == null) {
                currValues = new HashSet<>();
                m_valueMap.put(fieldName, currValues);
            }
            currValues.add(value);
            Utils.require(!fieldName.equals("_ID") || currValues.size() == 1, "Only 1 value can be set for _ID");
        }
    }   // addFieldValue

    /**
     * Clear all values for the given field. Both "add" and "remove" values, if any, are
     * deleted.
     * 
     * @param fieldName Name of a field.
     */
    public void clearLinkValues(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        m_valueMap.remove(fieldName);
        m_valueRemoveMap.remove(fieldName);
    }   // clearLinkValues
    
    /**
     * Parse an object rooted at the given "doc" UNode. All values parsed are stored in
     * the object. An exception is thrown if the node structure is incorrect or a field
     * has an invalid value.
     * 
     * @param docNode   Root node of a "doc" UNode structure.
     */
    public void parse(UNode docNode) {
        Utils.require(docNode != null, "docNode");
        Utils.require(docNode.getName().equals("doc"), "'doc' node expected: %s", docNode.getName());
        Utils.require(docNode.isMap(), "'doc' node must be a map of unique names: %s", docNode);
        
        for (String fieldName : docNode.getMemberNames()) {
            UNode fieldValue = docNode.getMember(fieldName);
            parseFieldUpdate(fieldName, fieldValue);
        }
    }   // parse

    /**
     * Add "remove" values for the given field. This method should only be called for MV
     * scalar and link fields. When the updates in this DBObject are applied to the
     * database, the given set of values are removed for the object. An exception is
     * thrown if the field is not MV. If the given set of values is null or empty, this
     * method is a no-op.
     * 
     * @param fieldName Name of an MV field to remove" values for.
     * @param values    Collection values to remove for the field.
     */
    public void removeFieldValues(String fieldName, Collection<String> values) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        if (values != null && values.size() > 0) {
            Set<String> valueSet = m_valueRemoveMap.get(fieldName);
            if (valueSet == null) {
                valueSet = new HashSet<String>();
                m_valueRemoveMap.put(fieldName, valueSet);
            }
            valueSet.addAll(values);
        }
    }   // removeFieldValues

    /**
     * Set this object's ID to the given string. If this DBObject already has an ID
     * assigned, it is replaced with the given value. This is a convenience method sets
     * the _ID field.
     * 
     * @param objID     The object's new ID.
     */
    public void setObjectID(String objID) {
        Set<String> values = m_valueMap.get(CommonDefs.ID_FIELD);
        if (values == null) {
            values = new HashSet<>();
            m_valueMap.put(CommonDefs.ID_FIELD, values);
        } else {
            values.clear();
        }
        values.add(objID);
    }   // setObjectID
    
    /**
     * Set the given field's value. It is intended to be used for SV scalar fields.
     * Compared to {@link #addFieldValues(String, Collection)} and
     * {@link #addFieldValue(String, String)}, this method replaces any current value(s)
     * by the given value. Also, with this method the given value can be null or empty to
     * nullify the field when the update is processed. 
     * 
     * @param fieldName     Name of field.
     * @param value         Value to replace or add to existing scalar's value(s).
     */
    public void setFieldValue(String fieldName, String value) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        
        Set<String> values = m_valueMap.get(fieldName);
        if (values == null) {
            values = new HashSet<>();
            m_valueMap.put(fieldName, values);
        } else {
            values.clear();
        }
        if (Utils.isEmpty(value)) {
            values.add("");
        } else {
            values.add(value);
        }
    }   // setFieldValue
    
    /**
     * Set the given field to the given set of values. It is intended to be used for MV
     * fields.  Compared to {@link #addFieldValues(String, Collection)} and
     * {@link #addFieldValue(String, String)}, this method first clears any current
     * value(s) for the given field.
     * 
     * @param fieldName     Name of field.
     * @param values        One or more values to replace existing scalar's value(s).
     */
    public void setFieldValues(String fieldName, Collection<String> values) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        
        Set<String> currValues = m_valueMap.get(fieldName);
        if (currValues == null) {
            currValues = new HashSet<>();
            m_valueMap.put(fieldName, currValues);
        } else {
            currValues.clear();
        }
        currValues.addAll(values);
    }   // setFieldValue
    
    ///// Private methods for UNode handling
    
    // Create a UNode for the leaf field with the given name and add it to the given
    // parent node.
    private void leafFieldtoDoc(UNode parentNode, String fieldName) {
        assert parentNode != null;
        
        Set<String> addSet = null;
        if (m_valueMap.containsKey(fieldName)) {
            addSet = new TreeSet<String>(m_valueMap.get(fieldName));
        }
        Set<String> removeSet = m_valueRemoveMap.get(fieldName);
        if (addSet != null && addSet.size() == 1 && removeSet == null) {
            parentNode.addValueNode(fieldName, addSet.iterator().next(), "field");
        } else {
            UNode fieldNode = parentNode.addMapNode(fieldName, "field");
            if (addSet != null && addSet.size() > 0) {
                UNode addNode = fieldNode.addArrayNode("add");
                for (String value : addSet) {
                    addNode.addValueNode("value", value);
                }
            }
            if (removeSet != null && removeSet.size() > 0) {
                UNode addNode = fieldNode.addArrayNode("remove");
                for (String value : removeSet) {
                    addNode.addValueNode("value", value);
                }
            }
        }
    }   // leafFieldtoDoc
    
    // Create a UNode for the given group field and add it to the given parent node.
    // Add child nodes for fields that belong to the group that are included in the
    // given deferred-field map. Recurse is any child fields are also groups.
    private void groupFieldtoDoc(UNode                parentNode,
                                 FieldDefinition      groupFieldDef,
                                 Set<FieldDefinition> deferredFields) {
        // Prerequisities:
        assert parentNode != null;
        assert groupFieldDef != null && groupFieldDef.isGroupField();
        assert deferredFields != null && deferredFields.size() > 0;
        
        UNode groupNode = parentNode.addMapNode(groupFieldDef.getName(), "field");
        for (FieldDefinition nestedFieldDef : groupFieldDef.getNestedFields()) {
            if (!deferredFields.contains(nestedFieldDef)) {
                continue;
            }
            if (nestedFieldDef.isGroupField()) {
                groupFieldtoDoc(groupNode, nestedFieldDef, deferredFields);
            } else {
                leafFieldtoDoc(groupNode, nestedFieldDef.getName());
            }
        }
    }   // groupFieldtoDoc

    // Parse update to outer or nested field.
    private void parseFieldUpdate(String fieldName, UNode valueNode) {
        Utils.require(!hasFieldValue(fieldName), "Duplicate assignment to field: %s", fieldName);
        if (valueNode.isValue()) {
            setFieldValue(fieldName, valueNode.getValue());
        } else {
            for (UNode childNode : valueNode.getMemberList()) {
                if (childNode.isCollection() && childNode.getName().equals("add") && childNode.hasMembers()) {
                    // "add" for an MV field
                    parseFieldAdd(fieldName, childNode);
                } else if (childNode.isCollection() && childNode.getName().equals("remove") && childNode.hasMembers()) {
                    // "remove" for an MV field
                    parseFieldRemove(fieldName, childNode);
                } else {
                    parseFieldUpdate(childNode.getName(), childNode);
                }
            }
        }
    }   // parseFieldUpdate

    // Parse an "add" update to a field.
    private void parseFieldAdd(String fieldName, UNode addNode) {
        Set<String> addValueSet = new HashSet<>();
        for (UNode valueNode : addNode.getMemberList()) {
            Utils.require(valueNode.isValue() && valueNode.getName().equals("value"),
                          "Value expected for 'add' element: " + valueNode);
            addValueSet.add(valueNode.getValue());
        }
        addFieldValues(fieldName, addValueSet);
    }   // parseFieldAdd

    // Parse an "add" update to a field.
    private void parseFieldRemove(String fieldName, UNode addNode) {
        Set<String> removeValueSet = new HashSet<>();
        for (UNode valueNode : addNode.getMemberList()) {
            Utils.require(valueNode.isValue() && valueNode.getName().equals("value"),
                          "Value expected for 'remove' element: " + valueNode);
            removeValueSet.add(valueNode.getValue());
        }
        removeFieldValues(fieldName, removeValueSet);
    }   // parseFieldRemove

}   // class DBObject
