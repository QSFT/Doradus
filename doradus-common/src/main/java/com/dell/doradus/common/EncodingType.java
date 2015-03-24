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
 * Defines encoding types we recognize, currently only used by {@link FieldType#BINARY}
 * fields.
 */
public enum EncodingType {
    BASE64(),
    HEX();
    
    /**
     * Return the EncodingType with the given string value (case-insensitive) or null if
     * the value is not recognized.
     * 
     * @param  value Candidate encoding type mnemonic such as "base64" or "HEX".
     * @return       EncodingType enumeration for the corresponding mnemonic or null if
     *               the value is not recognized.
     */
    public static EncodingType fromString(String value) {
        try {
            return valueOf(value.toUpperCase());
        } catch (Exception e) {
            // Could be IllegalArgumentException or NullPointerException.
            return null;
        }
    }   // fromString

    /**
     * Get the default encoding used when no explicit encoding is specified.
     * 
     * @return  Default encoding, currently {@link #BASE64}.
     */
    public static EncodingType getDefaultEncoding() {
        return BASE64;
    }   // getDefaultEncoding

    /**
     * Decode the given String value to its binary encoding according to this object's
     * encoding type.
     *  
     * @param   fieldValue  String value, encoded as required by this encoding type.
     * @return              Decoded binary value of string.
     * @throws IllegalArgumentException If the given string value is invalid.
     */
    public byte[] decode(String fieldValue) throws IllegalArgumentException {
        switch (this) {
        case BASE64:
            return Utils.base64ToBinary(fieldValue);
        case HEX:
            return Utils.hexToBinary(fieldValue);
        }
        return null;
    }   // decode
    
    /**
     * Encode the given binary value according to this object's encoding type.
     * 
     * @param   value   Binary value.
     * @return          String form of binary value using this object's encoding.
     */
    public String encode(byte[] value) {
        switch (this) {
        case BASE64:
            return Utils.base64FromBinary(value);
        case HEX:
            return Utils.hexFromBinary(value);
        }
        return null;
    }   // encode
    
    /**
     * Return a display-friendly string for this EncodingType enumeration. For example,
     * the BASE64 encoding returns the string "Base64".
     * 
     * @return A display-friendly string for this EncodingType enumeration.
     */
    @Override
    public String toString() {
        switch (this) {
        case BASE64:
            return "Base64";
        case HEX:
            return "Hex";
        }
        return null;
    }   // toString
    
}   // enum EncodingType
