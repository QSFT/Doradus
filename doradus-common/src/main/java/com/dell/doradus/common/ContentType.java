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
 * Simple class that encapsulates a content-type field and its accompanying charset for
 * access as a single object.
 */
final public class ContentType {
    // Some common ContentType objects for ease-of-use. All use UTF-8 by default.
    public static final ContentType TEXT_XML = new ContentType("text/xml");
    public static final ContentType APPLICATION_JSON = new ContentType("application/json");
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final ContentType TEXT_PLAIN = new ContentType("text/plain");;
    
    // Member variables:
    private final String m_contentType;
    private final String m_charset;
    
    /**
     * Create a ContentType value by parsing the content-type and charset values from the
     * value of a Content-Type HTTP header. For example, if the header is:
     * <pre>
     *      Content-Type: text/xml; charset=UTF-8
     * </pre>
     * The value "text/xml; charset=UTF-8" can be passed here, and the object will be
     * constructed with content-type=text/xml and charset=UTF-8.
     * 
     * @param contentTypeValue  Value of a Content-Type header.
     */
    public ContentType(String contentTypeValue) {
        assert contentTypeValue != null && contentTypeValue.length() > 0;
        
        // See if a semicolon is used.
        int inx = contentTypeValue.indexOf(';');
        if (inx < 0) {
            // No charset definition. Assume UTF-8 (for now).
            m_contentType = contentTypeValue.trim();
            m_charset = DEFAULT_CHARSET;
        } else {
            // Separate and trim.
            m_contentType = contentTypeValue.substring(0, inx).trim();
            String suffix = contentTypeValue.substring(inx + 1).trim();

            if (Utils.startsWith(suffix.toLowerCase(), "charset=")) {
                // Parameter is charset, so use it.
            	int len = "charset=".length();
            	if(len < suffix.length())
            		m_charset = suffix.substring("charset=".length());
            	else
            		m_charset = DEFAULT_CHARSET;
            } else {
                // Don't recognize suffix, so assume "UTF-8".
                m_charset = DEFAULT_CHARSET;
            }
        }
    }   // constructor
    
    /**
     * For ContentType, "equals" means same MIME type.
     * 
     * @param other Object to compare to.
     * @return      True if the given object is a ContentType with the same MIME type as
     *              this one.
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ContentType)) {
            return false;
        }
        
        // We may allow multiple XML or JSON content-types to be the "same", so we test
        // for these first and then for general text matching.
        ContentType otherCT = (ContentType)other;
        return (otherCT.isXML() && this.isXML()) ||
               (otherCT.isJSON() && this.isJSON()) ||
               (otherCT.m_contentType.equalsIgnoreCase(this.m_contentType));
    }   // equals
    
    /**
     * Return the hash code of this ContentType so it can be used as a key in collections,
     * etc. hashCode() generally must be overridden when equals() is. For our purposes,
     * we just use the hashcode of the content-type value.
     * 
     * @return Hash code unique to this object's content-type.
     */
    @Override
    public int hashCode() {
        return m_contentType.hashCode();
    }   // hashCode
    
    /**
     * Get the content-type value of this ContentType object.
     * @return  E.g., "text/xml".
     */
    public String getType() {
        return m_contentType;
    }   // getType
    
    /**
     * Get the charset value of this ContentType object.
     * @return  E.g., "UTF-8".
     */
    public String getCharset() {
        return m_charset;
    }   // getCharset
    
    /**
     * Return true if this type MIME type is considered JSON.
     * 
     * @return True if this type MIME type is considered JSON.
     */
    public boolean isJSON() {
        // We may add more if experience suggests we need them.
        return m_contentType.equalsIgnoreCase("application/json");
    }   // isJSON
    
    /**
     * Return true if this type MIME type is text/plain.
     * 
     * @return True if this type MIME type is text/plain.
     */
    public boolean isPlainText() {
        return m_contentType.equalsIgnoreCase("text/plain");
    }   // isPlainText
    
    /**
     * Return true if this type MIME type is considered XML.
     * 
     * @return True if this type MIME type is considered XML.
     */
    public boolean isXML() {
        // We may add more if experience suggests we need them.
        return m_contentType.equalsIgnoreCase("text/xml");
    }   // isXML
    
    /**
     * Return a string representing this ContentType object in a format suitable as a
     * header value. Specifically: {content-type}; Charset={charset}.
     * 
     * @return E.g.: text/xml; Charset=UTF-8
     */
    @Override
    public String toString() {
        return m_contentType + "; Charset=" + m_charset;
    }   // toString

}   // class ContentType

