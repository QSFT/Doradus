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
import java.io.Writer;

/**
 * Creates JSON documents. The no-parameter constructor creates an internal StringWriter
 * and all JSON text is written to it. After calling link #endDocument()} call,
 * {@link #toString()} can be called to get the JSON text. The constructor that accepts
 * a Writer argument pushes all JSON text to the given writer. When {@link #endDocument()}
 * is called, flush() is called on the given writer (and toString() should not be called).
 * <br>
 * Most methods return "this" to support call chaining. Example:
 * <pre>
 *         JSONEmitter json = new JSONEmitter();
 *         json.startDocument()
 *             .startGroup("doc")
 *             .startArray("fields")
 *             .addObject("Title", "Zen and the Art of Motorcycle Maintenance")
 *             .addObject("Amazon-Link", "http://www.amazon.com/")
 *             .addObject("Author", "Robert Pirsig")
 *             .addObject("Publisher", "Harper Perennial Modern Classics")
 *             .endArray()
 *             .endGroup()
 *             .endDocument();
 *         String text = json.toString();
 *         System.out.println(text);
 * </pre>
 */
final public class JSONEmitter {
    // Maximum nested construct depth we support:
    private static final int MAX_STACK_DEPTH = 32;
    
    // Members:
    private final Writer        m_writer;
    private final boolean[]     m_commaStack = new boolean[MAX_STACK_DEPTH];
    private       int           m_stackInx;
    private final int           m_indent;
    private final StringBuilder m_buffer = new StringBuilder();
    
    /**
     * Create a JSONEmitter that writes JSON text to an internal StringWriter. After
     * {@link #endDocument()} is called, the final JSON text an be obtained by calling
     * {@link #toString()}.
     */
    public JSONEmitter() {
        m_writer = new StringWriter();
        m_indent = 0;
    }   // constructor
    
    /**
     * Create a JSONEmitter that writes JSON text to an internal StringWriter. After
     * {@link #endDocument()} is called, the final JSON text an be obtained by calling
     * {@link #toString()}. If the given indent value is &gt; 0, the JSON formatting is
     * "pretty printed" using the given indentation.
     * 
     * @param indent    Indentation level for pretty printing. 0 disables pretty printing.
     */
    public JSONEmitter(int indent) {
        m_writer = new StringWriter();
        m_indent = indent;
    }   // constructor
    
    /**
     * Create a JSONEmitter that writes JSON text to the given Writer. When
     * {@link #endDocument()} is called, flush() is called on the writer to ensure all
     * characters are flushed.
     * 
     * @param writer    Writer to which JSON text is written.
     */
    public JSONEmitter(Writer writer) {
        m_writer = writer;
        m_indent = 0;
    }   // constructor
    
    /**
     * Start a new document. This causes the opening '{' to be emitted.
     * 
     * @return The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter startDocument() {
        write('{');
        push();
        return this;
    }   // startDocument

    /**
     * End the current document. This should be called only when the last object has been
     * ended. It causes the closing '}' to be emitted.
     * 
     * @return The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter endDocument() {
        pop('}');
        try {
            m_writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }   // endDocument

    /**
     * Start a new group object. The group emits the JSON:
     * <pre>
     *      "[name]":{
     * </pre>
     * 
     * @param name  Name used for this group.
     * @return      The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter startGroup(String name) {
        checkComma();
        write('"');
        write(encodeString(name));
        write("\":{");
        push();
        return this;
    }   // startGroup
    
    /**
     * Start an unnamed object. This emits the JSON text '{'.
     * 
     * @return      The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter startObject() {
        checkComma();
        write('{');
        push();
        return this;
    }   // startObject
    
    /**
     * End the current group object. This emits the group's closing '}'.
     * 
     * @return The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter endGroup() {
        // Remove this group from the stack and create a JsonRootNode that owns the
        // fields.
        pop('}');
        return this;
    }   // endGroup
    
    /**
     * Start a new array object using the given name. This emits the JSON text:
     * <pre>
     *      "[name]":[
     * </pre>
     * 
     * @param  name Name of new array object.
     * @return      The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter startArray(String name) {
        checkComma();
        write('"');
        write(encodeString(name));
        write("\":[");
        push();
        return this;
    }   // startArray
    
    /**
     * End the current array object, which emits the closing ']'.
     * 
     * @return The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter endArray() {
        pop(']');
        return this;
    }   // endArray

    /**
     * End the current unnamed object, which emits a close '}'.
     * 
     * @return      The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter endObject() {
        pop('}');
        return this;
    }   // endObject
    
    /**
     * Add a one-field object consisting of the given name and string value. This
     * generates the JSON text:
     * <pre>
     *      {"name":"value"}
     * </pre>
     * 
     * @param name  Name of new field.
     * @param value Value of new field as a string.
     * @return      The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter addObject(String name, String value) {
        checkComma();
        write("{\"");
        write(encodeString(name));
        write("\":\"");
        if (value != null) {
            write(encodeString(value));
        }
        write("\"}");
		return this;
  }   // addObject
    
    /**
     * Add a named value. This generates the JSON text:
     * <pre>
     *      "name":"value"
     * </pre>
     * 
     * @param name  Name of new field.
     * @param value Value of new field as a string.
     * @return      The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter addValue(String name, String value) {
        checkComma();
        write('"');
        write(encodeString(name));
        write("\":\"");
        if (value != null) {
            write(encodeString(value));
        }
        write('"');
        return this;
    }   // addValue
    
    /**
     * Add a String value.
     * 
     * @param   value String value.
     * @return        The same JSONEmitter object, which allows call chaining.
     */
    public JSONEmitter addValue(String value) {
        checkComma();
        write('"');
        write(encodeString(value));
        write('"');
        return this;
    }   // addValue

    /**
     * Return the JSON text for the JSON structure built by this JSONEmitter. The document
     * must have been finished via an {@link #endDocument()} call.
     * 
     * @return JSON text built by this builder.
     */
    @Override
    public String toString() {
        assert m_writer instanceof StringWriter;
        return m_writer.toString();
    }   // toString

    ///// Private methods
    
    private String encodeString(String str) {
        m_buffer.setLength(0);
        char ch;
        int inx = 0;
        while (inx < str.length()) {
            ch = str.charAt(inx++);
            switch (ch) {
            case '"':  m_buffer.append("\\\""); break;
            case '\\': m_buffer.append("\\\\"); break;
            case '\b':  m_buffer.append("\\b"); break;
            case '\f':  m_buffer.append("\\f"); break;
            case '\n':  m_buffer.append("\\n"); break;
            case '\r':  m_buffer.append("\\r"); break;
            case '\t':  m_buffer.append("\\t"); break;
            default:
                // All other chars are allowed as-is, despite JSON spec.
                m_buffer.append(ch);
            }
        }
        return m_buffer.toString();
    }   // encodeString
    
    // Add comma if needed, otherwise set need for one.
    private void checkComma() {
        if (m_commaStack[m_stackInx - 1]) {
            write(',');
        } else {
            m_commaStack[m_stackInx - 1] = true;
        }
        checkNewline();
    }   // checkComma
    
    // If we're indenting, emit a newline and indentation for next line.
    private void checkNewline() {
        if (m_indent > 0) {
            write("\n");
            write(' ', m_stackInx * m_indent);
        }
    }   // checkNewLine
    
    // Decrement tos by 1.
    private void pop(char closer) {
        assert m_stackInx > 0;
        m_stackInx--;
        if (m_commaStack[m_stackInx]) {
            checkNewline();
        }
        write(closer);
    }   // pop
    
    // Increment tos by 1 and set top-level need-comma to false.
    private void push() {
        assert m_stackInx < MAX_STACK_DEPTH;
        m_commaStack[m_stackInx++] = false;
    }   // push
    
    // Write a character and turn any IOException caught into a RuntimeException
    private void write(char ch) {
        try {
            m_writer.write(ch);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }   // write
    
    // Write a character repeat number of times and turn any IOException caught into a
    // RuntimeException.
    private void write(char ch, int repeat) {
        try {
            for (int i = 0; i < repeat; i++) {
                m_writer.write(ch);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }   // write
    
    // Write a string and turn any IOException caught into a RuntimeException
    private void write(String str) {
        try {
            m_writer.write(str);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }   // write
    
    ///// Main (for testing/demo)
    
    public static void main(String args[]) {
        // Here's an example of how to use JSONEmitter. This produces the following JSON:
        //     {"doc": {
        //         "fields": [
        //             {"Title": "Zen and the Art of Motorcycle Maintenance"},
        //             {"Amazon-Link": "http://www.amazon.com/"},
        //             {"Author": "Robert Pirsig"},
        //             {"Publisher": "Harper Perennial Modern Classics"}
        //         ]
        //     }}
        // Note: Not necessarily correct Doradus syntax!
        JSONEmitter json = new JSONEmitter();
        json.startDocument()
            .startGroup("doc")
            .startArray("fields")
            .addObject("Title", "Zen and the Art of Motorcycle Maintenance")
            .addObject("Amazon-Link", "http://www.amazon.com/")
            .addObject("Author", "Robert\tPirsig")
            .addObject("Publisher", "Harper Perennial Modern Classics\r\n")
            .endArray()     // fields
            .endGroup()    // doc
            .endDocument();
        String text = json.toString();
        System.out.println(text);
    }   // main

}   // class JSONEmitter
