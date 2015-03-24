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

import java.io.StringWriter;
import java.util.Map;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Creates XML documents by writing to a StringWriter.
 */
final public class XMLBuilder {
    private final static Attributes EMPTY_ATTS = new AttributesImpl();
    private final Stack<String> m_tagStack = new Stack<String>();
    private final StringWriter m_stringWriter = new StringWriter();
    private final int m_indent;
    private final String m_prefix;

    // Default constructor creates an XML Builder that generates the unformatted XML.
    // Use XMLBuilder(indent) constructor to generate the formatted XML.
    public XMLBuilder(){
    	this(0);
    }
    
    // Creates an XMLBuilder that generates the formatted XML
    public XMLBuilder(int indent) {
        m_indent = indent;
        StringBuilder buffer = new StringBuilder();
        for (int index = 0; index < indent; index++) {
            buffer.append(' ');
        }
        m_prefix = buffer.toString();
    }

    // Start a new XML document.
    public void startDocument() {
        m_stringWriter.write("<?xml version=\"1.0\" standalone=\"yes\"?>\n");
        m_tagStack.clear();
    }   // startDocument
    
    // Finish the current XML document.
    public void endDocument() {
        if (m_tagStack.size() != 0) {
            throw new RuntimeException("XML 'endDocument' with unfinished tags");
        }
        m_stringWriter.write('\n');
    }   // endDocument
    
    // Start a new XML element using the given start tag only.
    public void startElement(String elemName) {
        writeStartElement(elemName, EMPTY_ATTS);
        m_tagStack.push(elemName);
    }   // startElement
    
    // Start a new XML element using the given start tag and single attribute.
    public void startElement(String elemName, String attrName, String attrValue) {
        AttributesImpl attrs = new AttributesImpl();
        attrs.addAttribute("", attrName, "", "CDATA", attrValue);
        writeStartElement(elemName, attrs);
        m_tagStack.push(elemName);
    }   // startElement

    // Start a new XML element using the given name and attribute set.
    public void startElement(String elemName, Attributes attrs) {
        writeStartElement(elemName, attrs);
        m_tagStack.push(elemName);
    }   // startElement

    // Same as above but using an attribute map.
    public void startElement(String elemName, Map<String,String> attrs) {
        startElement(elemName, toAttributes(attrs));
    }   // startElement

    // Finish the outer-most XML element that was started.
    public void endElement() {
        if (m_tagStack.size() == 0) {
            throw new RuntimeException("XML 'endElement' with no unfinished tags");
        }
        writeEndElement(m_tagStack.pop());
    }   // endElement
    
    // Add an element, including start and end tags, with the content as text data within.
    public void addDataElement(String elemName, String content) {
        writeDataElement(elemName, EMPTY_ATTS, content);
    }   // addDataElement
    
    // Same as above but with a single attribute name/value pair as well.
    public void addDataElement(String elemName, String content, String attrName, String attrValue) {
        AttributesImpl attrs = new AttributesImpl();
        attrs.addAttribute("", attrName, "", "CDATA", attrValue);
        writeDataElement(elemName, attrs, content);
    }   // addDataElement

    // Same as above but using an attribute set.
    public void addDataElement(String elemName, String content, Attributes attrs) {
        writeDataElement(elemName, attrs, content);
    }   // addDataElement

    // Same as above but using an attribute map.
    public void addDataElement(String elemName, String content, Map<String,String> attrs) {
    	addDataElement(elemName, content, toAttributes(attrs));
    }   // addDataElement
    
    @Override
    public String toString() {
        if (m_tagStack.size() != 0) {
            throw new RuntimeException("Stack is not empty");
        }
        return m_stringWriter.toString();
    }   // toString

    // Converts attribute map to Attributes class instance.
    private Attributes toAttributes(Map<String,String> attrs){
        AttributesImpl impl = new AttributesImpl();
        for (Map.Entry<String,String> attr : attrs.entrySet()){
        	impl.addAttribute("", attr.getKey(), "", "CDATA", attr.getValue());
        }
        return impl;
    }   // toAttributes
    
    private void writeStartElement(String elemName, Attributes atts) {
        for (int level = 0; level < m_tagStack.size(); level++) {
            m_stringWriter.write(m_prefix);
        }
        m_stringWriter.write('<');
        m_stringWriter.write(elemName);
        writeAttributes(atts);
        m_stringWriter.write('>');
        if (m_indent > 0) {
            m_stringWriter.write('\n');
        }
    }
    
    private void writeEndElement(String elemName) {
        for (int level = 0; level < m_tagStack.size(); level++) {
            m_stringWriter.write(m_prefix);
        }
        m_stringWriter.write("</");
        m_stringWriter.write(elemName);
        m_stringWriter.write('>');
        if (m_indent > 0) {
            m_stringWriter.write('\n');
        }
    }
    
    private void writeDataElement(String elemName, Attributes atts, String content) {
        startElement(elemName, atts);
        writeCharacters(content);
        endElement();
    }

    private void writeAttributes(Attributes atts) {
        for (int i = 0; i < atts.getLength(); i++) {
            char value[] = atts.getValue(i).toCharArray();
            m_stringWriter.write(' ');
            m_stringWriter.write(atts.getLocalName(i));
            m_stringWriter.write("=\"");
            writeEscaped(value, 0, value.length, true);
            m_stringWriter.write('"');
        }
    }

    public void writeCharacters(String data)  {
        for (int level = 0; level < m_tagStack.size(); level++) {
            m_stringWriter.write(m_prefix);
        }
        char ch[] = data.toCharArray();
        writeCharacters(ch, 0, ch.length);
        if (m_indent > 0) {
            m_stringWriter.write('\n');
        }
    }
    
    public void writeCharacters(char ch[], int start, int len) {
        writeEscaped(ch, start, len, false);
    }
    
    private void writeEscaped(char ch[], int start, int length, boolean isAttVal) {
        for (int i = start; i < start + length; i++) {
            switch (ch[i]) {
            case '&':
                m_stringWriter.write("&amp;");
                break;
            case '<':
                m_stringWriter.write("&lt;");
                break;
            case '>':
                m_stringWriter.write("&gt;");
                break;
            case '\"':
                if (isAttVal) {
                    m_stringWriter.write("&quot;");
                } else {
                    m_stringWriter.write('\"');
                }
                break;
            default:
                if (ch[i] > '\u007f') {
                    m_stringWriter.write("&#");
                    m_stringWriter.write(Integer.toString(ch[i]));
                    m_stringWriter.write(';');
                } else {
                    m_stringWriter.write(ch[i]);
                }
            }
        }
    }

}   // class XMLBuilder
