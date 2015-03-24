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

/**
 * Field Types allowed by Doradus.
 */
public enum FieldType {
    // These types are not considered scalar:
    LINK(1),
    GROUP(2),
    XLINK(3),
    
    // These are scalar types:
    TEXT(10),
    BINARY(11),
    BOOLEAN(12),
    INTEGER(13),
    LONG(14),
    TIMESTAMP(15),
    FLOAT(16),
    DOUBLE(17);
    
    ///// Members
    
    // We explicitly define the ordinal so it can be persisted:
    private int m_value;
    
    // Constructor
    private FieldType(int value) {
        m_value = value;
    }   // constructor
    
    /**
     * Return true if this field type is any link type: LINK or XLINK.
     * 
     * @return  True if this field type is any link type: LINK or XLINK.
     */
    public boolean isLinkType() {
        return this == LINK || this == XLINK;
    }   // isLinkType
    
    /**
     * Return true if this field type is considered a scalar type.
     * 
     * @return  True if this field type is considered a scalar type.
     */
    public boolean isScalarType() {
        return this.m_value >= 10;
    }   // isScalarType

    /**
     * Return the FieldType with the given string value (case-insensitive) or null if
     * there is no such field type.
     * 
     * @param  value Candidate field type mnemonic such as "link" or "text".
     * @return       FieldType enumeration for the corresponding mnemonic or null if
     *               there is no such field type.
     */
    public static FieldType fromString(String value) {
        try {
            return valueOf(value.toUpperCase());
        } catch (Exception e) {
            // Could be IllegalArgumentException or NullPointerException.
            return null;
        }
    }   // fromString

    /**
     * Get the default analyzer name for the given field type. Only the simple name is
     * returned (e.g., "TextAnalyzer" or "NullAnalyzer"). This method can only be called
     * for scalar types.
     * 
     * @param fieldType Scalar field type.
     * @return          Default analyzer name for the field.
     */
    public static String getDefaultAnalyzer(FieldType fieldType) {
        // Type must be scalar
        Utils.require(fieldType.isScalarType(), "Analyzer not valid for this field type: " + fieldType);
        
        switch (fieldType) {
        case BINARY:
        case DOUBLE:
        case FLOAT:
            return "NullAnalyzer";
        case BOOLEAN:
            return "BooleanAnalyzer";
        case INTEGER:
        case LONG:
            // Integer and long both use the IntegerAnalzyer
            return "IntegerAnalyzer";
        case TEXT:
            return "TextAnalyzer";
        case TIMESTAMP:
            return "DateAnalyzer";
        default:
            throw new IllegalArgumentException("FieldType does not have a default analyzer: " + fieldType);
        }   // switch
    }   // getDefaultAnalyzer

}   // enum FieldType
