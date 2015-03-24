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

import java.util.SortedMap;
import java.util.TreeMap;

/** 
 * Holds the definition of a field within a table.
 */
final public class FieldDefinition {
    // Members that make-up a field.
    private TableDefinition   m_tableDef;           // may change during schema processing
    private FieldType         m_type;
    private String            m_name;
    private boolean           m_bIsCollection;      // true => MV scalar field
    private String            m_analyzerName;
    private boolean           m_bIsSharded;         // true => sharded link field
    private String            m_junctionField;      // for xlink fields only
    
    // If the field is a Link, the following members also apply:
    private String m_linkInverse;
    private String m_linkExtent;    // table in which linked objects reside

    // If the field is binary, an encoding is required:
    private EncodingType m_encoding;
    
    // If the field is a Group, its immediate nested fields are in this map:
    private final SortedMap<String, FieldDefinition> m_nestedFieldMap =
        new TreeMap<String, FieldDefinition>();
    
    // If this field is a nested Field, this points to the parent field:
    private FieldDefinition m_parentField;
    
    /**
     * Indicate if the given field name is valid. Currently, field names must begin with a
     * letter and consist of all letters, digits, and underscores.
     * 
     * @param fieldName Candidate field name.
     * @return          True if the given name is valid for fields.
     */
    public static boolean isValidFieldName(String fieldName) {
        return fieldName != null &&
               fieldName.length() > 0 &&
               Utils.isLetter(fieldName.charAt(0)) &&
               Utils.allAlphaNumUnderscore(fieldName);
    }   // isValidFieldName
    
    /**
     * Create an empty FieldDefinition that belongs to the given table. The object is not
     * valid until a name and type are set, e.g., via parsing.
     * 
     * @param tableDef  {@link TableDefinition} of table to which FieldDefinition belongs.
     */
    public FieldDefinition(TableDefinition tableDef) {
        assert tableDef != null;
        m_tableDef = tableDef;
    }   // constructor
    
    /**
     * Parse the field definition rooted at the given UNode and store the definition in
     * this object. The given UNode is the field node, hence its name is the field name
     * and its chuildren are field attributes such as "type" and and "inverse". An
     * exception is thrown if the definition is invalid.
     * 
     * @param fieldNode     UNode that defines a field.
     */
    public void parse(UNode fieldNode) {
        assert fieldNode != null;
        
        // Node must be a map or a VALUE with a name only (empty table def).
        Utils.require(fieldNode.isMap() || (fieldNode.isValue() && Utils.isEmpty(fieldNode.getValue())),
                      "'field' definition must be a map of unique names: " + fieldNode);
        
        // Verify the field name and save it.
        Utils.require(isValidFieldName(fieldNode.getName()),
                      "Invalid field name: " + fieldNode.getName());
        m_name = fieldNode.getName();
        
        // Specifies if the collection attribute was explicitly set:
        boolean bCollectionSet = false;
        
        // Parse the nodes child nodes. If we find a "fields" definition, just save it
        // for later.
        UNode nestedFieldsNode = null;
        String analyzerName = null;
        for (String childName : fieldNode.getMemberNames()) {
            // See if we recognize it.
            UNode childNode = fieldNode.getMember(childName);
            
            // "type"
            if (childName.equals("type")) {
                // Value must be a string.
                Utils.require(childNode.isValue(),
                              "Value of 'type' must be a string: " + childNode);
                Utils.require(m_type == null,
                              "'type' can only be specified once");
                m_type = FieldType.fromString(childNode.getValue());
                Utils.require(m_type != null,
                              "Unrecognized field 'type': " + childNode.getValue());
                
            // "collection"
            } else if (childName.equals("collection")) {
                // Value must be a string.
                Utils.require(childNode.isValue(),
                              "Value of 'collection' must be a string: " + childNode);
                m_bIsCollection = Utils.getBooleanValue(childNode.getValue());
                bCollectionSet = true;
                
            // "analyzer"
            } else if (childName.equals("analyzer")) {
                // Value must be a string.
                Utils.require(childNode.isValue(),
                              "Value of 'analyzer' must be a string: " + childNode);
                Utils.require(analyzerName == null,
                              "'analyzer' can only be specified once");
                analyzerName = childNode.getValue();
            
            // "inverse"
            } else if (childName.equals("inverse")) {
                // Value must be a string.
                Utils.require(childNode.isValue(),
                              "Value of 'inverse' must be a string: " + childNode);
                Utils.require(m_linkInverse == null,
                              "'inverse' can only be specified once");
                m_linkInverse = childNode.getValue();
                
            // "table"
            } else if (childName.equals("table")) {
                // Value must be a string.
                Utils.require(childNode.isValue(),
                              "Value of 'table' must be a string: " + childNode);
                Utils.require(m_linkExtent == null,
                              "'table' can only be specified once");
                m_linkExtent = childNode.getValue();
                
            // "fields"
            } else if (childName.equals("fields")) {
                // This field must be (or can become) a group.
                Utils.require(m_type == null || m_type == FieldType.GROUP,
                              "Only group fields can have nested elements: " + m_name);
                
                // Value must be an map.
                Utils.require(childNode.isMap(),
                              "Value of 'fields' must be map of unique names: " + childNode);
                Utils.require(nestedFieldsNode == null,
                              "'fields' can only be specified once: " + m_name);

                // Save the node for later processing.
                nestedFieldsNode = childNode;
                
            // "sharded"
            } else if (childName.equals("sharded")) {
                // Value must be a string.
                Utils.require(childNode.isValue(),
                              "Value of 'sharded' must be a string: " + childNode);
                m_bIsSharded = Utils.getBooleanValue(childNode.getValue());
            
            // "encoding"
            } else if (childName.equals("encoding")) {
                Utils.require(childNode.isValue(),
                              "Value of 'encoding' must be a string: " + childNode);
                m_encoding = EncodingType.fromString(childNode.getValue());
                Utils.require(m_encoding != null, "Unrecognized 'encoding': " + childNode.getValue());
                
            // "junction"
            } else if (childName.equals("junction")) {
                Utils.require(childNode.isValue(), "Value of 'junction' must be a string: " + childNode);
                m_junctionField = childNode.getValue();
                
            // Unrecognized.
            } else {
                Utils.require(false, "Unrecognized field attribute: " + childName);
            }
        }

        // If we didn't get a 'type', default to "text".
        if (m_type == null) {
            if (nestedFieldsNode == null) {
                m_type = FieldType.TEXT;
            } else {
                m_type = FieldType.GROUP;
            }
        }
        
        // If an 'inverse' or 'table' was specified, type must be LINK or XLINK.
        Utils.require(m_linkInverse == null || m_type.isLinkType(),
                      "'inverse' not allowed for this field type: " + m_name);
        Utils.require(m_linkExtent == null || m_type.isLinkType(),
                      "'table' not allowed for this field type: " + m_name);
        
        // LINK and XLINK require an 'inverse'.
        Utils.require(!m_type.isLinkType() || m_linkInverse != null,
                      "Missing 'inverse' option: " + m_name);
        
        // LINK and XLINK 'table' defaults to same table.
        if (m_type.isLinkType() && Utils.isEmpty(m_linkExtent)) {
            m_linkExtent = m_tableDef.getTableName();
        }
        
        // XLINK requires a junction field: default is "_ID".
        if (!Utils.isEmpty(m_junctionField)) {
            Utils.require(m_type == FieldType.XLINK, "'junction' is only allowed for xlinks");
        } else if (m_type == FieldType.XLINK) {
            m_junctionField = "_ID";
        }
        
        // If collection was not explicitly set, set to true for links and false for scalars
        if (!bCollectionSet) {
            m_bIsCollection = m_type.isLinkType();
        }
        
        // Set or verify 'analyzer' for scalar field types.
        if (analyzerName != null) {
            // Assure this is a scalar, but don't verify the analyzer here.
            Utils.require(m_type.isScalarType(),
                          "'analyzer' can only be specified for scalar field types: " + analyzerName);
            m_analyzerName = analyzerName;
        }
        
        // If this is a binary field, ensure "encoding" is set.
        if (m_encoding != null) {
            Utils.require(m_type == FieldType.BINARY, "'encoding' is only valid for binary fields");
        } else {
            m_encoding = EncodingType.getDefaultEncoding();
        }
        
        // Binary fields cannot be collections.
        Utils.require(m_type != FieldType.BINARY || !m_bIsCollection,
                      "Binary fields cannot be collections (multi-valued)");
        
        // If this is a group, ensure nested fields were declared.
        if (m_type == FieldType.GROUP) {
            Utils.require(nestedFieldsNode != null &&
                          nestedFieldsNode.hasMembers(),
                          "Group field must have at least one nested field defined: " + m_name);
            for (String nestedFieldName : nestedFieldsNode.getMemberNames()) {
                // Create a FieldDefinition for the nested field and parse details into it.
                UNode nestedFieldNode = nestedFieldsNode.getMember(nestedFieldName);
                FieldDefinition nestedField = new FieldDefinition(m_tableDef);
                nestedField.parse(nestedFieldNode);
                addNestedField(nestedField);
            }
        } else {
            Utils.require(nestedFieldsNode == null, "Only group fields can have nested elements: " + m_name);
        }
    }   // parse(UNode)
    
    ///// Getters

    /**
     * Get the analyzer name for this field. An exception is thrown for non-scalar fields.
     * 
     * @return The analyzer name for this field.
     */
    public String getAnalyzerName() {
        // Should only be asking for analyzer for a scalar field.
        assert isScalarField();
        return m_analyzerName;
    }   // getAnalyzer
    
    /**
     * Get this field's encoding as an {@link EncodingType}. Only binary fields have
     * encoding, so null is returned for all other types.
     * 
     * @return  This (binary) field's encoding, or null if it is not binary.
     */
    public EncodingType getEncoding() {
        return m_encoding;
    }   // getEncoding
    
    /**
     * Return the name of the table to which this field belongs.
     * 
     * @return Name of table to which this field belongs.
     */
    public String getTableName() {
        return m_tableDef.getTableName();
    }   // getTableName
    
    /**
     * Return the {@link TableDefinition} for the table that owns this field.
     * 
     * @return This table's {@link TableDefinition}.
     */
    public TableDefinition getTableDef() {
        return m_tableDef;
    }   // getTableDef
    
    /**
     * Return this field's {@link FieldType}.
     * 
     * @return This field's {@link FieldType}.
     */
    public FieldType getType() {
        return m_type;
    }   // getType
    
    /**
     * Return this field's name.
     * 
     * @return  This field's name.
     */
    public String getName() {
        return m_name;
    }   // getName

    /**
     * Return true if this field's type is binary, meaning {@link FieldType#BINARY}.
     * 
     * @return  True if this field's type is binary.
     */
    public boolean isBinaryField() {
        return m_type == FieldType.BINARY;
    }   // isBinaryField

    /**
     * Indicate if this field is an MV scalar field, also known as a scalar collection.
     * 
     * @return  True if this field is a scalar collection.
     */
    public boolean isCollection() {
        return m_bIsCollection;
    }   // isCollection
    
    /**
     * Indicate if this field definition is a group field.
     * 
     * @return  True if this field definition is a group field.
     */
    public boolean isGroupField() {
        return m_type == FieldType.GROUP;
    }   // isGroupField

    /**
     * Indicate if this field is sharded. Currently, this will be true only for link
     * fields whose target table is sharded.
     * 
     * @return  True if this is a sharded link field.
     */
    public boolean isSharded() {
        return m_bIsSharded;
    }   // isSharded
    
    /**
     * Indicate if this field definition defines a Link field.
     * 
     * @return  True if this is a Link field definition.
     * @see #isLinkType()
     * @see #isXLinkField()
     */
    public boolean isLinkField() {
        return m_type == FieldType.LINK;
    }   // isLinkField

    /**
     * Indicate if this field definition's type is <b>any</b> link type:
     * {@link FieldType#LINK} or {@link FieldType#XLINK}.
     * 
     * @return  True if this is a Link field definition.
     * @see #isLinkField()
     * @see #isXLinkField()
     */
    public boolean isLinkType() {
        return m_type.isLinkType();
    }   // isLinkType
    
    /**
     * Indicate if this field definition defines an XLINK field.
     * 
     * @return  True if this is a XLINK field definition.
     * @see #isLinkField()
     * @see #isLinkType()
     */
    public boolean isXLinkField() {
        return m_type == FieldType.XLINK;
    }   // isXLinkField

    /**
     * Indicate if this XLINK field is direct, i.e. goes with junction field 
     * @return  True if this is a direct XLINK field with junction field defined.
     * @see #isXLinkField()
     * @see #getXLinkJunction()
     */
    public boolean isXLinkDirect() {
        return isXLinkField() && !CommonDefs.ID_FIELD.equals(getXLinkJunction());
    }

    /**
     * Indicate if this XLINK field is inverse, i.e. its inverse is a direct XLink 
     * @return  True if this is a inverse XLINK field with junction field equal to _ID
     * @see #isXLinkField()
     * @see #getXLinkJunction()
     */
    public boolean isXLinkInverse() {
        return isXLinkField() && CommonDefs.ID_FIELD.equals(getXLinkJunction());
    }
    
    /**
     * Indicate if this field's type is a scalar type (integer, text, timestamp, etc.)
     * 
     * @return True if this field's type is scalar.
     */
    public boolean isScalarField() {
        return m_type.isScalarType();
    }   // isScalarField
    
    /**
     * Get the name of this link field definition's "inverse" link field. If this field
     * definition is not a link, null is returned.
     * 
     * @return  This link field's inverse field name or null if it is not a link field.
     */
    public String getLinkInverse() {
        return m_linkInverse;
    }   // getLinkInverse

    /**
     * Get the {@link TableDefinition} of this field's extent table, which is the table
     * that owns its inverse link. This field definition must be a link field. Null is
     * returned if the inverse table has not been defined in this application.
     * 
     * @return  {@link TableDefinition} of this link's inverse link's owning table or
     *          null if it has not been defined in this application.
     */
    public TableDefinition getInverseTableDef() {
        assert isLinkType();
        return m_tableDef.getAppDef().getTableDef(m_linkExtent);
    }   // getInverseTableDef

    /**
     * Get the {@link FieldDefinition} of this field's inverse link definition. This field
     * must be a link field. Null is returned if either the inverse table or inverse link
     * have not been defined in this application.
     * 
     * @return  {@link FieldDefinition} of this link field's inverse link field or null if
     *          the inverse table or link have not been defined in this application.
     */
    public FieldDefinition getInverseLinkDef() {
        assert isLinkType();
        TableDefinition inverseTableDef = getInverseTableDef();
        if (inverseTableDef == null) {
            return null;
        }
        return inverseTableDef.getFieldDef(m_linkInverse);
    }   // getInverseLinkDef
    
    /**
     * Get the name of the table that holds objects referenced by this link field
     * definition, which is called the "extent". If this field definition is not a link,
     * null is returned.
     * 
     * @return  This link field's inverse field name or null if it is not a link field.
     */
    public String getLinkExtent() {
        return m_linkExtent;
    }   // getLinkExtent
    
    /**
     * Get the {@link FieldDefinition} for the nested field with the given name from this
     * group field. If this field is not a group or does not contain the specified nested
     * field, null is returned.
     * 
     * @param   fieldName   Candidate nested field name.
     * @return              True if this is a group that contains the given nested field
     *                      name.
     */
    public FieldDefinition getNestedField(String fieldName) {
        if (m_type != FieldType.GROUP) {
            return null;
        }
        return m_nestedFieldMap.get(fieldName);
    }   // getNestedField

    /**
     * If this is a group field, return the nested {@link FieldDefinition} objects as an
     * {@link Iterable} collection. If this is not a group field, null is returned.
     * 
     * @return  Nested field definitions as an Iterable collection if this is a group
     *          field, otherwise null.
     */
    public Iterable<FieldDefinition> getNestedFields() {
        if (m_type != FieldType.GROUP) {
            return null;
        }
        assert m_nestedFieldMap != null;
        assert m_nestedFieldMap.size() > 0;
        return m_nestedFieldMap.values();
    }   // getNestedFields
    
    /**
     * If this field is nested within a group field, get the {@link FieldDefinition} of
     * the owning group. If this field is not a nested field, null is returned.
     *  
     * @return  The parent group's {@link FieldDefinition} if any, otherwise null.
     */
    public FieldDefinition getParentField() {
        return m_parentField;
    }   // getParentField
    
    /**
     * Get the junction field name of an xlink field. The junction field is the field that
     * holds a foreign key on which the xlink is based.
     * 
     * @return The field name of this xlink's junction field.
     */
    public String getXLinkJunction() {
        assert m_type == FieldType.XLINK;
        return m_junctionField;
    }   // getXLinkJunction
    
    /**
     * Indicate if this group field contains an immediated-nested field with the given
     * name. If this field is not a group, false is returned.
     * 
     * @param   fieldName   Name of candidate nested field.
     * @return              True if this is a group field that contains the given name
     *                      as an immediated nested field.
     */
    public boolean hasNestedField(String fieldName) {
        if (m_type != FieldType.GROUP) {
            return false;
        }
        return m_nestedFieldMap.containsKey(fieldName);
    }   // hasNestedField
    
    /**
     * Indicate if this field is nested within a group field.
     * 
     * @return  True if this field is nested within a group field.
     */
    public boolean isNestedField() {
        return getParentField() != null;
    }   // isNestedField
    
    // Return "Field 'foo': LINK". This is for debugging.
    @Override
    public String toString() {
        return "Field '" + m_name + "': " + m_type;
    }   // toString
    
    ///// Setters

    /**
     * Add the given FieldDefiniton as a child (nested field) of this field. This method
     * also sets the given field's "parent field" pointer to point to us.
     * 
     * @param nestedFieldDef    {@link FieldDefinition} of a scalar field to become a
     *                          nested field of this group.
     */
    public void addNestedField(FieldDefinition nestedFieldDef) {
        // Prerequisites
        assert nestedFieldDef != null;
        assert m_type == FieldType.GROUP;
        assert !m_nestedFieldMap.containsKey(nestedFieldDef.getName());
        
        // Add it to us and point it at us.
        m_nestedFieldMap.put(nestedFieldDef.getName(), nestedFieldDef);
        nestedFieldDef.m_parentField = this;
        
        // Ensure the nested field is owned by the same TableDefinition in case
        nestedFieldDef.setOwningTable(m_tableDef);
    }   // addNestedField

    /**
     * Set the analyzer name for this field to the given value. This can only be called
     * for scalar fields.
     * 
     * @param analyzerName  New analyzer name for this field.
     */
    public void setAnalyzer(String analyzerName) {
        assert m_type.isScalarType();
        assert analyzerName != null;
        m_analyzerName = analyzerName;
    }   // setAnalyzer
    
    /**
     * Set this field's collection property to the given value. When a field is a
     * collection, it is multi-valued. Non-collection fields are single-valued.
     * 
     * @param isCollection  New value for this field's collection property.
     */
    public void setCollection(boolean isCollection) {
        m_bIsCollection = isCollection;
    }   // setCollection
    
    /**
     * Set the field's inverse property to the given field name.
     * 
     * @param linkInverse   Name of inverse link field for this link field.
     */
    public void setLinkInverse(String linkInverse) {
        assert linkInverse != null && linkInverse.length() > 0;
        m_linkInverse = linkInverse;
    }   // setLinkInverse

    /**
     * Set the field's extent property to the given table name.
     * 
     * @param linkExtent    Name of inverse link's table.
     */
    public void setLinkExtent(String linkExtent) {
        assert linkExtent != null && linkExtent.length() > 0;
        m_linkExtent = linkExtent;
    }   // setLinkExtent

    /**
     * Set this field's name to the given valid. An IllegalArgumentException is thrown
     * if the name is already assigned or is not valid.
     * 
     * @param fieldName New name for field.
     */
    public void setName(String fieldName) {
        if (m_name != null) {
            throw new IllegalArgumentException("Field name is already set: " + m_name);
        }
        if (!isValidFieldName(fieldName)) {
            throw new IllegalArgumentException("Invalid field name: " + fieldName);
        }
        m_name = fieldName;
    }   // setName
    
    /**
     * Change the table that owns this field to the given {@link TableDefinition}. This
     * method should only be used to "transfer" ownership of a table during schema
     * processing, e.g., when a new field is parsed in an existing table and is "moved"
     * to the existing TableDefinition when the schema change is committed.
     * 
     * @param tableDef  {@link TableDefinition} of field's new owning table.
     */
    public void setOwningTable(TableDefinition tableDef) {
        // Table name should not change.
        assert m_tableDef.getTableName().equals(tableDef.getTableName());
        m_tableDef = tableDef;
    }   // setOwningTable
    
    /**
     * Set this field's type to the given FieldType. An IllegalArgumentException is thrown
     * if the type is already assigned.
     * 
     * @param type  New {@link FieldType} for this FieldDefinition. 
     */
    public void setType(FieldType type) {
        if (m_type != null) {
            throw new IllegalArgumentException("Field type is already set: " + m_type);
        }
        m_type = type;
    }   // setType

    /**
     * Set the junction for this xlink field to the given field name. This field must be a
     * {@link FieldType#XLINK}. The given field name must be "_ID" or the name of a text
     * field defined in the same table.
     *  
     * @param fieldName Name of junction field to use for this xlink field.
     */
    public void setXLinkJunction(String fieldName) {
        assert m_type == FieldType.XLINK;
        m_junctionField = fieldName;
    }   // setXLinkJunction

    /**
     * Serialize this field definition into a {@link UNode} tree and return the root node.
     * 
     * @return  This field definition serialized into a UNode tree.
     */
    public UNode toDoc() {
        // Root node is a MAP and its name is the field name. Set its tag name to "field"
        // for XML.
        UNode fieldNode = UNode.createMapNode(m_name, "field");
        
        // Groups and non-groups are handled differently.
        if (m_type != FieldType.GROUP) {
            // Non-group: Add a "type" attribute, marked as an XML attribute.
            fieldNode.addValueNode("type", m_type.toString(), true);
            
            // Add 'collection', marked as an XML attribute.
            if (m_type.isLinkType() || m_bIsCollection) {
                fieldNode.addValueNode("collection", Boolean.toString(m_bIsCollection), true);
            }
            
            // Add 'analyzer' attribute, if specified, marked as an XML attribute.
            if (m_analyzerName != null) {
                fieldNode.addValueNode("analyzer", m_analyzerName, true);
            }
            
            // Add 'encoding' if applicable, marked as an XML attribute.
            if (m_type == FieldType.BINARY) {
                fieldNode.addValueNode("encoding", m_encoding.toString(), true);
            }
            
            // If this field is a link, add the 'inverse' and 'table' attributes, each
            // marked as XML attributes. Add the link field number only if it is being used.
            if (m_type.isLinkType()) {
                fieldNode.addValueNode("inverse", m_linkInverse, true);
                fieldNode.addValueNode("table", m_linkExtent, true);
            }
                
            // Add 'sharded' for link if true.
            if (m_type == FieldType.LINK && m_bIsSharded) {
                fieldNode.addValueNode("sharded", Boolean.toString(m_bIsSharded), true);
            }
            
            // Add 'junction' for xlink.
            if (m_type == FieldType.XLINK) {
                fieldNode.addValueNode("junction", m_junctionField, true);
            }
        } else {
            // Create a MAP node called "fields".
            assert m_nestedFieldMap.size() > 0;
            UNode fieldsNode = fieldNode.addMapNode("fields");
            
            // Recursing to nested fields, adding them as child nodes.
            for (FieldDefinition nestedFieldDef : getNestedFields()) {
                fieldsNode.addChildNode(nestedFieldDef.toDoc());
            }
        }
        return fieldNode;
    }   // toDoc

}   // FieldDefinition
