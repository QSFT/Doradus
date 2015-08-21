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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Holds field values for a Doradus database object. Both user-defined and system-defined
 * fields (_ID, _table, _deleted, _shard) may be present. When a DBObject is created by
 * fetching an object from the database, the _ID values is always present. Additional
 * scalar and/or link field values are present if requested and if a value was found.
 * The system fields _table and _deleted are only used for certain update operations. The
 * system field _shard is only used in certain OLAP queries.
 * Field values can be retrieved via these methods:
 * <pre>
 *      {@link #getFieldNames()}
 *      {@link #getFieldValue(String)}      // when field has a single value
 *      {@link #getFieldValues(String)}
 *      {@link #getObjectID()}
 *      {@link #isDeleted()}
 *      {@link #getTableName()}
 * </pre>
 * 
 * When used to add a new object, a DBObject holds values to be added for both scalar and
 * link fields. A DBObject does not need an object ID if it is new and should receive a
 * unique, system-generated ID. To update an existing object, an ID must be defined.
 * The user and system fields are assigned via the methods:
 * 
 * <pre>
 *      {@link #addFieldValue(String, String)}
 *      {@link #addFieldValues(String, Collection)}
 *      {@link #setObjectID(String)}
 *      {@link #setDeleted(boolean)}
 *      {@link #setTableName(String)}
 * </pre>
 * 
 * A DBObject that is used to update an existing object can also define values to be
 * removed for existing MV scalar and link fields. Values to be removed from a field are
 * assigned via this method:
 * 
 * <pre>
 *      {@link #removeFieldValues(String, Collection)}
 * </pre>
 * 
 * To determine which fields have been assigned new values, "remove" values, or either,
 * use these methods:
 * 
 * <pre>
 *      {@link #getFieldNames()}
 *      {@link #getRemoveValues(String)}
 *      {@link #getUpdatedFieldNames()}
 *      {@link #getUpdatedScalarFieldNames(TableDefinition)}
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
final public class DBObject implements JSONable{
    // Known system field keys:
    private static final String _ID = "_ID";  
    private static final String _TABLE = "_table";
    private static final String _SHARD = "_shard";
    private static final String _DELETED = "_deleted";
    
    // Members:
    private final Map<String, List<String>> m_valueMap = new LinkedHashMap<>();
    private final Map<String, List<String>> m_valueRemoveMap = new HashMap<>();
    
    /**
     * Create a new, empty DBObject.
     */
    public DBObject() {}
    
    /**
     * Create a new DBObject with the given _ID and _table values.
     * 
     * @param objID     New object's ID (_ID field).
     * @param tableName New object's table (_table field).
     */
    public DBObject(String objID, String tableName) {
        if (!Utils.isEmpty(objID)) {
            setObjectID(objID);
        }
        if (!Utils.isEmpty(tableName)) {
            setTableName(tableName);
        }
    }   // constructor
    
    /**
     * Make a copy of this object with the same updates and values but with a new object
     * ID.
     * 
     * @param   objID   Object IF of new object.
     * @return          Copy of this object except for the object ID.
     */
    public DBObject makeCopy(String objID) {
        DBObject newObj = new DBObject();
        newObj.m_valueMap.putAll(m_valueMap);
        newObj.setObjectID(objID);
        newObj.m_valueRemoveMap.putAll(m_valueRemoveMap);
        return newObj;
    }   // makeCopy
    
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
        HashSet<String> result = new LinkedHashSet<>(m_valueMap.keySet());
        return result;
    }   // getFieldNames
    
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
     * Get this object's ID or null if no object ID has yet been assigned.
     * 
     * @return  This DBObject's ID or null if none has been assigned.
     */
    public String getObjectID() {
        return getSystemField(_ID);
    }   // getObjectID
    
    /**
     * Get this object's _shard field, if any.
     * 
     * @return  This object's shard name, if any, otherwise null.
     */
    public String getShardName() {
        return getSystemField(_SHARD);
    }   // getShardName
    
    /**
     * Get this object's assigned table name if any.
     * 
     * @return  This object's table name, if any.
     */
    public String getTableName() {
        return getSystemField(_TABLE);
    }   // getTableName
    
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
        
        List<String> values = m_valueMap.get(fieldName);
        if (values == null || values.size() == 0) {
            return null;
        }
        Utils.require(values.size() == 1, "Field has more than 1 value: %s", fieldName);
        return values.get(0);
    }   // getFieldValue
    
    /**
     * Get all values of the field with the given name or null if no values are assigned
     * to the given field. If the field has at least one value, the list returned is *not*
     * copied. This means it may have duplicate values as well.
     * 
     * @param  fieldName    Name of a field.
     * @return              Set of all values assigned to the field.
     */
    public List<String> getFieldValues(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        
        return m_valueMap.get(fieldName);
    }   // getFieldValues
    
    /**
     * Get this object's deleted flag, if set.
     * 
     * @return  This DBObject's deleted flag, if set through {@link #setDeleted(boolean)}
     *          or parsed from a UNode tree.
     */
    public boolean isDeleted() {
        String value = getSystemField(_DELETED);
        return value == null ? false : Boolean.parseBoolean(value);
    }   // isDeleted
    
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
     * Add the value(s) in the given collection to the field with the given name. If the
     * given collection is null or empty, this method is a no-op.
     * 
     * @param fieldName Name of field.
     * @param values    Collection of values. Ignored if null or empty.
     */
    public void addFieldValues(String fieldName, Collection<String> values) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        
        if (values != null && values.size() > 0) {
            if (fieldName.charAt(0) == '_') {
                Utils.require(values.size() == 1, "System fields can have only 1 value: " + fieldName);
                setSystemField(fieldName, values.iterator().next());
            } else {
                List<String> currValues = m_valueMap.get(fieldName);
                if (currValues == null) {
                    currValues = new ArrayList<>();
                    m_valueMap.put(fieldName, currValues);
                }
                currValues.addAll(values);
            }
        }
    }   // addFieldValues

    /**
     * Add the given value to the field with the given name. For a system field, its
     * existing value, if any, is replaced. If a null or empty field is added for an
     * SV scalar, the field is nullified.
     * 
     * @param fieldName Name of a field.
     * @param value     Value to add to field. Ignored if null or empty.
     */
    public void addFieldValue(String fieldName, String value) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        
        if (fieldName.charAt(0) == '_') {
            setSystemField(fieldName, value);
        } else {
            List<String> currValues = m_valueMap.get(fieldName);
            if (currValues == null) {
                currValues = new ArrayList<>();
                m_valueMap.put(fieldName, currValues);
            }
            currValues.add(value);
        }
    }   // addFieldValue

    /**
     * Clear all values for the given field. Both "add" and "remove" values, if any, are
     * deleted.
     * 
     * @param fieldName Name of a field.
     */
    public void clearValues(String fieldName) {
        Utils.require(!Utils.isEmpty(fieldName), "fieldName");
        m_valueMap.remove(fieldName);
        m_valueRemoveMap.remove(fieldName);
    }   // clearValues
    
    /**
     * Parse an object rooted at the given "doc" UNode. All values parsed are stored in
     * the object. An exception is thrown if the node structure is incorrect or a field
     * has an invalid value.
     * 
     * @param docNode   Root node of a "doc" UNode structure.
     * @return          This object.
     */
    public DBObject parse(UNode docNode) {
        Utils.require(docNode != null, "docNode");
        Utils.require(docNode.getName().equals("doc"), "'doc' node expected: %s", docNode.getName());
        
        for (String fieldName : docNode.getMemberNames()) {
            UNode fieldValue = docNode.getMember(fieldName);
            parseFieldUpdate(fieldName, fieldValue);
        }
        return this;
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
            List<String> valueSet = m_valueRemoveMap.get(fieldName);
            if (valueSet == null) {
                valueSet = new ArrayList<String>();
                m_valueRemoveMap.put(fieldName, valueSet);
            }
            valueSet.addAll(values);
        }
    }   // removeFieldValues

    /**
     * Set this object's ID to the given string. If this DBObject already has an ID
     * assigned, it is replaced with the given value.
     * 
     * @param objID     The object's new ID.
     */
    public void setObjectID(String objID) {
        setSystemField(_ID, objID);
    }   // setObjectID
    
    /**
     * Set this object's _shard field to the given value. If this DBObject already has a
     * _shard value, it is replaced. The _shard field is only used in certain operations.
     * 
     * @param shardName New value for _shard field.
     */
    public void setShardName(String shardName) {
        setSystemField(_SHARD, shardName);
    }   // setShardName
    
    /**
     * Set this object's _table field to the given value. If this DBObject already has a
     * _table value, it is replaced. The _table field is only used in certain operations.
     * 
     * @param tableName New value for _table field.
     */
    public void setTableName(String tableName) {
        setSystemField(_TABLE, tableName);
    }   // setTableName
    
    /**
     * Set this object's _deleted flag to the given value. Only certain operations use the
     * _deleted field.
     * 
     * @param bDeleted  New value for the _deleted field.
     */
    public void setDeleted(boolean bDeleted) {
        setSystemField(_DELETED, Boolean.toString(bDeleted));
    }   // setDeleted
    
    ///// Private methods for UNode handling
    
    // Create a UNode for the leaf field with the given name and add it to the given
    // parent node.
    private void leafFieldtoDoc(UNode parentNode, String fieldName) {
        assert parentNode != null;
        
        Set<String> addSet = null;
        if (m_valueMap.containsKey(fieldName)) {
            addSet = new TreeSet<String>(m_valueMap.get(fieldName));
        }
        List<String> removeSet = m_valueRemoveMap.get(fieldName);
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
        if (valueNode.isValue()) {
            addFieldValue(fieldName, valueNode.getValue());
        } else if (valueNode.childrenAreValues()) {
            parseFieldAdd(fieldName, valueNode);
        } else {
            for (UNode childNode : valueNode.getMemberList()) {
                if (childNode.isCollection() && childNode.getName().equals("add") && childNode.childrenAreValues()) {
                    // "add" for an MV field
                    parseFieldAdd(fieldName, childNode);
                } else if (childNode.isCollection() && childNode.getName().equals("remove") && childNode.childrenAreValues()) {
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
        Set<String> addValueSet = new LinkedHashSet<>();
        for (UNode valueNode : addNode.getMemberList()) {
            Utils.require(valueNode.isValue() && valueNode.getName().equals("value"),
                          "Value expected for 'add' element: " + valueNode);
            addValueSet.add(valueNode.getValue());
        }
        addFieldValues(fieldName, addValueSet);
    }   // parseFieldAdd

    // Parse an "add" update to a field.
    private void parseFieldRemove(String fieldName, UNode addNode) {
        Set<String> removeValueSet = new LinkedHashSet<>();
        for (UNode valueNode : addNode.getMemberList()) {
            Utils.require(valueNode.isValue() && valueNode.getName().equals("value"),
                          "Value expected for 'remove' element: " + valueNode);
            removeValueSet.add(valueNode.getValue());
        }
        removeFieldValues(fieldName, removeValueSet);
    }   // parseFieldRemove

    // Get the first value of the given field, or null if there is no value.
    private String getSystemField(String fieldName) {
        List<String> values = m_valueMap.get(fieldName);
        return values == null ? null : values.get(0);
    }   // getSystemField

    // Set the given field to the given single value. Note that the array returned by
    // Arrays.asList() cannot be expanded.
    private void setSystemField(String fieldName, String value) {
        if (Utils.isEmpty(value)) {
            m_valueMap.remove(fieldName);
        } else {
            m_valueMap.put(fieldName, Arrays.asList(value));
        }
    }   // setSystemField
    
    @Override
    public String toJSON() {
        return toDoc().toJSON();
    }
    
    ///// Builder
    
    /**
      * Creates a new {@link Builder} instance. This is a convenience method for
      * {@code new Builder()}.
      *
      * @return New {@link Builder}.
      */
     public static DBObject.Builder builder() {
         return new DBObject.Builder();
     }
     
     /**
      * Helper class to build {@link DBObject} instances.
      */
     public static class Builder {
        private DBObject dbObject = new DBObject();
                    
        /**
         * Return the completed {@link DBObject} instance. 
         *
         * @return Completed {@link DBObject} instance.
         */
        public DBObject build() {
            return this.dbObject;
        }

        /**
         * Set the object ID, replacing the current one if any.
         * 
         * @param objID Object ID.
         * @return      This {@link Builder}.
         */
        public Builder withID(String objID) {
            dbObject.setObjectID(objID);
            return this;
        }

        /**
         * Set the "_table" system field, replacing the current value if set. Only batches
         * that allow DBObjects from different tables require this property.
         *  
         * @param tableName New value for "_table" system field.
         * @return          This {@link Builder}.
         */
        public Builder withTable(String tableName) {
            dbObject.setTableName(tableName);
            return this;
        }
        
        /**
         * Set the "_deleted" system field, replacing the current value if set. Only
         * batches that allow a mixture of updated and deleted objects need this field to
         * identify objects to be deleted.
         * 
         * @param isDeleted New value for "_deleted" system field.
         * @return          This {@link Builder}.
         */
        public Builder withDeleted(boolean isDeleted) {
            dbObject.setDeleted(isDeleted);
            return this;
        }
        
        /**
         * Add the given string value to the scalar or link field with the given name.
         * Only one value should be assigned to SV scalar fields. If an empty or null
         * value is used for an SV scalar, the field is nullified if an existing object
         * is updated with this DBObject. Multiple values can be assigned to MV scalar and
         * link fields. For a link field, values must be ID(s) of object(s) to which the
         * link will point. Inverse link values are set automatically. 
         *  
         * @param fieldName     Name of scalar or link field.
         * @param fieldValue    String value to add to the field.
         * @return              This {@link Builder}.
         */
        public Builder withValue(String fieldName, String fieldValue) {
            dbObject.addFieldValue(fieldName, fieldValue);
            return this;
        }
        
        /**
         * Add the given value to the Boolean scalar field with the given name. Only
         * one value should be assigned to SV scalar fields. Multiple values can be
         * assigned to MV scalar fields.
         *  
         * @param fieldName     Name of Boolean scalar field.
         * @param fieldValue    Boolean value to add to the field.
         * @return              This {@link Builder}.
         */
        public Builder withValue(String fieldName, boolean fieldValue) {
            dbObject.addFieldValue(fieldName, Boolean.toString(fieldValue));
            return this;
        }
        
        /**
         * Add the given value to the numeric scalar field with the given name. Only
         * one value should be assigned to SV scalar fields. Multiple values can be
         * assigned to MV scalar fields.
         *  
         * @param fieldName     Name of a numberic scalar field (integer, long, float,
         *                      or double).
         * @param fieldValue    Long value to add to the field.
         * @return              This {@link Builder}.
         */
        public Builder withValue(String fieldName, long fieldValue) {
            dbObject.addFieldValue(fieldName, Long.toString(fieldValue));
            return this;
        }
        
        /**
         * Add the given value to the float or double scalar field with the given name.
         * Only one value should be assigned to SV scalar fields. Multiple values can be
         * assigned to MV scalar fields.
         *  
         * @param fieldName     Name of a float or double scalar field.
         * @param fieldValue    Double value to add to the field.
         * @return              This {@link Builder}.
         */
        public Builder withValue(String fieldName, double fieldValue) {
            dbObject.addFieldValue(fieldName, Double.toString(fieldValue));
            return this;
        }
        
        /**
         * Add the given Date value to the scalar field with the given name. This method
         * is intended for use with timestamp fields, though it can also be used with text
         * fields. Only one value should be assigned to SV scalar fields. Multiple values
         * can be assigned to MV scalar fields. The Date value is formatted into a string
         * in the format "YYYY-MM-DD hh:mm:ss.SSS" using the UTC time zone and then added
         * to the field. {@link Builder#withValue(String, String)} can also be used to add
         * values to a timestamp field using any of the allowed timestamp formats.
         *  
         * @param fieldName     Name of a timestamp or text scalar field.
         * @param fieldValue    Date value to add to the field.
         * @return              This {@link Builder}.
         */
        public Builder withValue(String fieldName, Date fieldValue) {
            Utils.require(fieldValue != null, "fieldValue");
            dbObject.addFieldValue(fieldName, Utils.formatDateUTC(fieldValue, Calendar.MILLISECOND));
            return this;
        }
        
        /**
         * Add an array of values to the MV scalar or link field with the given name. This
         * method works for any scalar field type, though values must be converted to
         * strings. For a link field, values must be IDs of objects to which the link will
         * point. Inverse link values are set automatically.
         *  
         * @param fieldName     Name of MV scalar or link field to update.
         * @param fieldValues   Array of one or more string values to add.
         * @return              This {@link Builder}.
         */
        public Builder withValues(String fieldName, String... fieldValues) {
            dbObject.addFieldValues(fieldName, Arrays.asList(fieldValues));
            return this;
        }
        
        /**
         * Add a collection of values to the MV scalar or link field with the given name.
         * This method works for any scalar field type, though values must be converted to
         * strings. For a link field, values must be IDs of objects to which the link will
         * point. Inverse link values are set automatically.
         *  
         * @param fieldName     Name of MV scalar or link field to update.
         * @param fieldValues   Collection of one or more string values to add.
         * @return              This {@link Builder}.
         */
        public Builder withValues(String fieldName, Collection<String> fieldValues) {
            dbObject.addFieldValues(fieldName, fieldValues);
            return this;
        }
        
        /**
         * Remove values from an MV scalar or link field. When the DBObject is applied as
         * an update to an existing object, the given values are removed, if present, from
         * the existing MV scalar or link field. Link field values must be object IDs.
         *  
         * @param fieldName     Name of MV scalar or link field to update.
         * @param fieldValues   Array of values to be removed.
         * @return              This {@link Builder}.
         */
        public Builder removeValues(String fieldName, String... fieldValues) {
            dbObject.removeFieldValues(fieldName, Arrays.asList(fieldValues));
            return this;
        }
        
        /**
         * Remove values from an MV scalar or link field. When the DBObject is applied as
         * an update to an existing object, the given values are removed, if present, from
         * the existing MV scalar or link field. Link field values must be object IDs.
         *  
         * @param fieldName     Name of MV scalar or link field to update.
         * @param fieldValues   Collection of values to be removed.
         * @return              This {@link Builder}.
         */
        public Builder removeValues(String fieldName, Collection<String> fieldValues) {
            dbObject.removeFieldValues(fieldName, fieldValues);
            return this;
        }
    }

}   // class DBObject
