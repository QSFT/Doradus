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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.megginson.sax.DataWriter;
import com.megginson.sax.XMLWriter;

/**
 * Creates XML documents using the com.megginson.sax.XMLWriter class. It also steals, er,
 * borrows ideas from com.dell.rogue.util.RogueMLWriter.
 * TODO: Currently this class does not stream: It builds everything up in a String and
 * outputs it with the toString() method.
 */
final public class XMLBuilder {
    // Stack of element names, so they don't have to be passed-in to endElement():
    private final Stack<String> m_tagStack = new Stack<String>();
    
    // Where the data is pushed (for now):
    private final StringWriter m_stringWriter = new StringWriter();

    // The XMLWriter, which does all the work:
    private XMLWriter m_xmlWriter;

    // Default constructor creates an XML Builder that generates the unformatted XML.
    // Use XMLBuilder(indent) constructor to generate the formatted XML.
    public XMLBuilder(){
    	this(0);
    }
    
    // Creates an XMLBuilder that generates the formatted XML
    public XMLBuilder(int indent){
    	if (indent == 0){
    		m_xmlWriter = new XMLWriter(m_stringWriter);    		
    	}
    	else {
    		m_xmlWriter = new DataWriter(m_stringWriter);
    		((DataWriter)m_xmlWriter).setIndentStep(indent);
    	}
    }

    // Start a new XML document.
    public void startDocument() throws IOException {
        try {
            m_xmlWriter.startDocument();
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
        m_tagStack.clear();
    }   // startDocument
    
    // Finish the current XML document.
    public void endDocument() throws IOException {
        if (m_tagStack.size() != 0) {
            throw new IOException("XML 'endDocument' with unfinished tags");
        }
        try {
            m_xmlWriter.endDocument();
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
    }   // endDocument
    
    // Start a new XML element using the given start tag only.
    public void startElement(String elemName) throws IOException {
        try {
            m_xmlWriter.startElement(elemName);
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
        m_tagStack.push(elemName);
    }   // startElement
    
    // Start a new XML element using the given start tag and single attribute.
    public void startElement(String elemName, String attrName, String attrValue) throws IOException {
        AttributesImpl attrs = new AttributesImpl();
        attrs.addAttribute("", attrName, "", "CDATA", attrValue);
        try {
            m_xmlWriter.startElement("", elemName, "", attrs);
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
        m_tagStack.push(elemName);
    }   // startElement

    // Start a new XML element using the given name and attribute set.
    public void startElement(String elemName, Attributes attrs) throws IOException {
        try {
            m_xmlWriter.startElement("", elemName, "", attrs);
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
        m_tagStack.push(elemName);
    }   // startElement

    // Same as above but using an attribute map.
    public void startElement(String elemName, Map<String,String> attrs) throws IOException {
        startElement(elemName, toAttributes(attrs));
    }   // startElement

    // Finish the outer-most XML element that was started.
    public void endElement() throws IOException {
        if (m_tagStack.size() == 0) {
            throw new IOException("XML 'endElement' with no unfinished tags");
        }
        try {
            m_xmlWriter.endElement(m_tagStack.pop());
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
    }   // endElement
    
    // Add an element, including start and end tags, with the content as text data within.
    public void addDataElement(String elemName, String content) throws IOException {
        try {
            m_xmlWriter.dataElement(elemName, content);
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
    }   // addDataElement
    
    // Same as above but with a single attribute name/value pair as well.
    public void addDataElement(String elemName, String content, String attrName, String attrValue)
            throws IOException {
        AttributesImpl attrs = new AttributesImpl();
        attrs.addAttribute("", attrName, "", "CDATA", attrValue);
        try {
            m_xmlWriter.dataElement("", elemName, "", attrs, content);
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
    }   // addDataElement

    // Same as above but using an attribute set.
    public void addDataElement(String elemName, String content, Attributes attrs) 
            throws IOException {
        try {
            m_xmlWriter.dataElement("", elemName, "", attrs, content);
        } catch (SAXException ex) {
            // Repackage as an IOException
            throw new IOException("Error building XML document", ex);
        }
    }   // addDataElement

    public void addEmptyElement(String elemName, Map<String,String> attrs) throws IOException{
    	addEmptyElement(elemName, toAttributes(attrs));
    }

    public void addEmptyElement(String elemName, Attributes attrs) throws IOException {
        try { 
        	m_xmlWriter.emptyElement("", elemName, "", attrs);
        	m_xmlWriter.characters("");
        } catch (SAXException ex) { throw new IOException("Error building XML document", ex); }
    }

    // Same as above but using an attribute map.
    public void addDataElement(String elemName, String content, Map<String,String> attrs) 
    		throws IOException{
    	addDataElement(elemName, content, toAttributes(attrs));
    }   // addDataElement
    
    @Override
    public String toString() {
        // We can't throw, so just assert.
        assert m_tagStack.size() == 0;
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
    
}   // class XMLBuilder
