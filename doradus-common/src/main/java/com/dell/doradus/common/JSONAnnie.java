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
import java.io.Reader;

/**
 * JSON parser that uses the SAX-style interface for JSON, aka "SAJ". As constructs are
 * recognized, appropriate methods in the {@link SajListener} object passed to
 * {@link #parse(SajListener)} are called. 
 * <p>
 * Note that this class supports only the subset of JSON that Doradus uses. As a result,
 * there are a few oddities, as described below:
 * <ul>
 * <li>Arrays within arrays are not supported. That is, <code>foo: [10, [11, 12]]</code>
 *     will throw an error when the inner array is seen.
 * </li>
 * <li>All Doradus messages use the general form <code>{"foo": {...stuff...}}</code>. The
 *     outer braces are silently parsed and ignore. When the sequence <code>"foo": {</code>
 *     is recognized, {@link SajListener#onStartObject(String)} is called passing "foo" as
 *     the object name.
 * </li>
 * <li>When the sequence <code>"foo": [</code> is recognized,
 *     {@link SajListener#onStartArray(String)} is called passing "foo" as the array name.
 * </li>
 * <li>When a simple object member value is recognized, e.g., <code>"name": "value"</code>,
 *     the method {@link SajListener#onValue(String, String)} is called passing "name" and
 *     "value". If the value is the JSON literal <code>null</code>, an empty string is
 *     passed as the value. Numbers are also passed as strings, as are the constants
 *     <code>true</code> and <code>false</code>.
 * </li>
 * <li>When a simple array value is recognized, e.g., "123" or "xyz", the method
 *     {@link SajListener#onValue(String, String)} is called passing "value" as the name
 *     and string form of value using the same rules as in the case above.
 * </li>
 * <li>When an array value is a "value object" as in the example <code>"foo": [{"bar":
 *     "bat"}]</code>, the outer curly braces are tossed, after the onArrayStart("foo")
 *     call, the next call will be onValue("bar", "bar").
 * </li>
 * <li>Similarly, if an array value is a complex object as in the example <code>"foo":
 *     [{"bar": {...stuff...}}]</code>, the outer curly braces are stripped. Instead,
 *     onObjectStart("bar") is called followed by whatever is appropriate based on the
 *     <code>...stuff...</code> inside the inner curly braces.
 * </li>
 * <li>{@link SajListener#onEndObject()} and {@link SajListener#onEndArray()} are called
 *     whenever a matching '}' or ']' is found (except for the ignore-curly-braces cases
 *     described above).
 * </li>
 * </ul>
 * <p>
 * Why Annie? I have a Jack Russel Terrier named Annie, and she is very fast.
 * 
 * @author Randy Guck
 */
public class JSONAnnie {
    /**
     * The listener interface for {@link JSONAnnie#parse(SajListener)} calls.
     */
    public interface SajListener {
        /**
         * This method is called when the sequence '"name": {' is recognized.
         * 
         * @param name  Member name that starts an object declaration.
         */
        void onStartObject(String name);
        
        /**
         * This method is called when the '}' is recognized that matches a
         * {@link #onStartObject(String)} call.
         */
        void onEndObject();

        /**
         * This method is called when the sequence '"name": [' is recognized.
         * 
         * @param name  Member name that starts a named array declaration.
         */
        void onStartArray(String name);
        
        /**
         * This method is called when the ']' is recognized that matches a
         * {@link #onStartArray(String)} call.
         */
        void onEndArray();
        
        /**
         * This method is called when the sequence '"name": "value"' is recognized or when
         * a simple unnamed array value is recognized. In the latter case, the name passed
         * is "value". When the value is JSON literal <i>null</i>, an empty string is
         * passed as the value. Numbers are also passed as strings, and the constants
         * <i>true</i> and <i>false</i> are passed as strings as well.
         * 
         * @param name  Name of named value of "value" for unnamed array values.
         * @param value Value parsed, converted to a String.
         */
        void onValue(String name, String value);
    }   // SajListener
    
    // Constants
    private static final int MAX_STACK_DEPTH = 32;
    private static final char EOF = (char)-1;

    // JSON text input source:
    private final JSONInput m_input;
    
    // Stack of nested JSON constructs:
    enum Construct {
        GHOST,      // { } being silently ignore
        OBJECT,     // { } passed to the listener
        ARRAY       // [ ] passed to the listener
    }
    private Construct[] m_stack = new Construct[MAX_STACK_DEPTH];
    private int         m_stackPos = 0;
    
    // Current parsing state:
    enum State {
        MEMBER_LIST,
        MEMBER,
        VALUE,
        NEXT,
    }
    private State state;

    ///// JSONInput classes
    
    // Abstract class that encapsulates a JSON input source. It provides methods to get the
    // next character, get the next non-whitespace character, and push a character back for
    // subsequent re-reading. It also provides methods to extract a complete JSON token:
    // string, number, or literal constant.
    private abstract static class JSONInput {
        // Short-term (single method) temporary buffers
        final StringBuilder buffer = new StringBuilder();
        final StringBuilder digitBuffer = new StringBuilder();
        
        // Return next char to parse or EOF
        abstract char nextChar(boolean isEOFAllowed);
        
        // Push back the last character. Only a 1-char pushback is needed.
        abstract void pushBack(char ch);
        
        // Return the next non-whitespace character or EOF. Throw is EOF is reached unexpectedly.
        char nextNonWSChar(boolean isEOFAllowed) {
            char ch = nextChar(isEOFAllowed);
            while (ch != EOF && Character.isWhitespace(ch)) {
                ch = nextChar(isEOFAllowed);
            }
            return ch;
        }   // nextNonWSChar

        // Return the quoted string beginning at the given char; error if we don't find one.
        String nextString(char ch) {
            // Decode escape sequences and return unquoted string value
            buffer.setLength(0);
            check(ch == '"', "'\"' expected: " + ch);
            while (true) {
                ch = nextChar(false);
                if (ch == '"') {
                    // Outer closing double quote.
                    break;
                } else if (ch == '\\') {
                    // Escape sequence.
                    ch = nextChar(false);
                    switch (ch) {
                    case 'u':
                        // \uDDDD sequence: Expect four hex digits.
                        digitBuffer.setLength(0);
                        for (int digits = 0; digits < 4; digits++) {
                            ch = nextChar(false);
                            check(Utils.isHexDigit(ch), "Hex digit expected: " + ch);
                            digitBuffer.append(ch);
                        }
                        buffer.append((char)Integer.parseInt(digitBuffer.toString(), 16));
                        break;
                    case '\"': buffer.append('\"'); break;
                    case '\\': buffer.append('\\'); break;
                    case '/':  buffer.append('/');  break;
                    case 'b':  buffer.append('\b'); break;
                    case 'f':  buffer.append('\f'); break;
                    case 'n':  buffer.append('\n'); break;
                    case 'r':  buffer.append('\r'); break;
                    case 't':  buffer.append('\t'); break;
                    default:
                        check(false, "Invalid escape sequence: \\" + ch);
                    }
                } else {
                    // All other chars are allowed as-is, despite JSON spec.
                    buffer.append(ch);
                }
            }
            return buffer.toString();
        }   // nextString
        
        // Parse a literal value beginning at the given char; error if we don't find one.
        String nextValue(char ch) {
            if (ch == '"') {
                return nextString(ch);
            }
            if (ch == '-' || (ch >= '0' && ch <= '9')) {
                return nextNumber(ch);
            }
            if (Character.isLetter(ch)) {
                return nextLiteral(ch);
            }
            check(false, "Unrecognized start of value: " + ch);
            return null;    // unreachable
        }   // nextValue
        
        // Parse a number literal. Currently we only recognize integers; not exponents.
        String nextNumber(char ch) {
            buffer.setLength(0);
            // First char only can be a "dash".
            if (ch == '-') {
                buffer.append(ch);
                ch = nextChar(false);
            }
            // Accumulate leading digits
            while (ch >= '0' && ch <= '9') {
                buffer.append(ch);
                ch = nextChar(false);
            }
            // Look for fractional part
            if (ch == '.') {
                buffer.append(ch);
                ch = nextChar(false);
                int fracDigits = 0;
                while (ch >= '0' && ch <= '9') {
                    fracDigits++;
                    buffer.append(ch);
                    ch = nextChar(false);
                }
                check(fracDigits > 0, "JSON fractional part requires at least one digit: " + buffer);
            }
            // Look for exponent
            if (ch == 'e' || ch == 'E') {
                buffer.append(ch);
                ch = nextChar(false);
                if (ch == '-' || ch == '+') {
                    buffer.append(ch);
                    ch = nextChar(false);
                }
                int expDigits = 0;
                while (ch >= '0' && ch <= '9') {
                    expDigits++;
                    buffer.append(ch);
                    ch = nextChar(false);
                }
                check(expDigits > 0, "JSON exponent part requires at least one digit: " + buffer);
            }
            // Push back the last non-digit.
            pushBack(ch);
            // Cannot be "-" only
            String value = buffer.toString();
            check(!value.equals("-"), "At least one digit must follow '-' in numeric value");
            return value;
        }   // nextNumber
        
        // Parse a keyword constant: false, true, or null. Case-insensitive.
        String nextLiteral(char ch) {
            buffer.setLength(0);
            while (Character.isLetter(ch)) {
                buffer.append(ch);
                ch = nextChar(false);
            }
            // Push back the last letter.
            pushBack(ch);
            String value = buffer.toString();
            if (value.equalsIgnoreCase("false")) {
                return "false";
            }
            if (value.equalsIgnoreCase("true")) {
                return "true";
            }
            if (value.equalsIgnoreCase("null")) {
                return "";
            }
            check(false, "Unrecognized literal: " + value);
            return null;    // not reachable
        }
    }   // class JSONInput
    
    // JSONInput specialization that reads chars from a String.
    private static class JSONInputString extends JSONInput {
        private int index;
        private final String jsonText;

        // Wrap the given string.
        JSONInputString(String text) {
            jsonText = text;
        }   // constructor
        
        // Return next char to parse or EOF
        @Override
        char nextChar(boolean isEOFAllowed) {
            if (index >= jsonText.length()) {
                check(isEOFAllowed, "Unexpected EOF");
                return EOF;
            }
            return jsonText.charAt(index++);
        }   // nextChar

        // Push back 1 char.
        @Override
        void pushBack(char ch) {
            assert index > 0;
            index--;
            assert jsonText.charAt(index) == ch;
        }   // pushBack
        
    }   // class JSONInputString
    
    // JSONInput specialization that reads chars from a reader.
    private static class JSONInputReader extends JSONInput {
        final Reader reader;
        int pushBack = -1;

        // Wrap the given Reader. Note that we don't close it when we're done.
        JSONInputReader(Reader reader) {
            this.reader = reader;
        }   // constructor
        
        // Use the push back char if present, otherwise use the Reader.
        @Override
        char nextChar(boolean isEOFAllowed) {
            int ch;
            if (pushBack != -1) {
                ch = pushBack;
                pushBack = -1;
            } else {
                try {
                    ch = reader.read();
                } catch (IOException e) {
                    ch = -1;
                }
            }
            check(ch != -1 || isEOFAllowed, "Unexpected EOF");
            return (char)ch;
        }   // nextChar

        // Push back 1 char, which must be read next.
        @Override
        void pushBack(char ch) {
            assert pushBack == -1 : "Only 1 char can be pushed back";
            pushBack = ch;
        }   // pushBack
    }   // class JSONInputReader
    
    ///// Public methods
    
    /**
     * Create an object that uses the given text as input when {@link #parse(SajListener)}
     * is called.
     * 
     * @param jsonText  JSON text string.
     */
    public JSONAnnie(String jsonText) {
        Utils.require(jsonText != null && jsonText.length() > 0, "JSON text cannot be empty");
        m_input = new JSONInputString(jsonText);
    }   // constructor
    
    /**
     * Create an object that uses the given reader as input when {@link #parse(SajListener)}
     * is called. Note that we don't close the Reader when we're done with it.
     * 
     * @param reader    Open character Reader. 
     */
    public JSONAnnie(Reader reader) {
        m_input = new JSONInputReader(reader);
    }   // constructor

    /**
     * Parse the JSON given to the constructor and call the given listener as each construct
     * is found. An IllegalArgumentException is thrown if a syntax error or unsupported JSON
     * construct is found.
     * 
     * @param  listener                 Callback for SAJ eve.ts
     * @throws IllegalArgumentException If there's a syntax error.
     */
    public void parse(SajListener listener) throws IllegalArgumentException {
        assert listener != null;
        
        // We require the first construct to be an object with no leading whitespace.
        m_stackPos = 0;
        char ch = m_input.nextChar(false);
        check(ch == '{', "First character must be '{': " + ch);
        
        // Mark outer '[' with a "ghost" object.
        push(Construct.GHOST);
        
        // Enter the state machine parsing the first member name.
        state = State.MEMBER_LIST;
        boolean bFinished = false;
        String memberName = null;
        String value = null;
        while (!bFinished) {
            switch (state) {
            case MEMBER_LIST:
                // Expect a quote to start a member or a '}' to denote an empty list.
                ch = m_input.nextNonWSChar(false);
                if (ch != '}') {
                    // Should a quote to start a member name.
                    state = State.MEMBER;
                } else {
                    // Empty object list
                    if (pop() == Construct.OBJECT) {
                        listener.onEndObject();
                    }
                    if (emptyStack()) {
                        bFinished = true;
                    }
                    state = State.NEXT;
                }
                break;
                
            case MEMBER:
                // Expect an object member: "name": <value>.
                memberName = m_input.nextString(ch);
                
                // Colon should follow
                ch = m_input.nextNonWSChar(false);
                check(ch == ':', "Colon expected: " + ch);
                
                // Member value shoud be next
                ch = m_input.nextNonWSChar(false);
                if (ch == '{') {
                    listener.onStartObject(memberName);
                    push(Construct.OBJECT);
                    state = State.MEMBER_LIST;
                } else if (ch == '[') {
                    listener.onStartArray(memberName);
                    push(Construct.ARRAY);
                    state = State.VALUE;
                } else {
                    // Value must be a literal.
                    value = m_input.nextValue(ch);
                    listener.onValue(memberName, value);
                    
                    // Must be followed by next member or object/array closure.
                    state = State.NEXT;
                }
                break;
                
            case VALUE:
                // Here, we expect an array value: object or literal.
                ch = m_input.nextNonWSChar(false);
                if (ch == '{') {
                    // Push a GHOST so we eat the outer { } pair.
                    push(Construct.GHOST);
                    state = State.MEMBER_LIST;
                } else if (ch == ']') {
                    // Empty array.
                    listener.onEndArray();
                    pop();
                    if (emptyStack()) {
                        bFinished = true;
                    } else {
                        state = State.NEXT;
                    }
                } else if (ch == '[') {
                    // Value is a nested array, which we don't currently support.
                    check(false, "Nested JSON arrays are not supported");
                } else {
                    // Value must be a literal value. It is implicitly named "value"
                    value = m_input.nextValue(ch);
                    listener.onValue("value", value);
                    
                    // Must be followed by next member or object/array terminator
                    state = State.NEXT;
                }
                break;
                
            case NEXT:
                // Expect a comma or object/array closure.
                ch = m_input.nextNonWSChar(false);
                Construct tos = tos();
                if (ch == ',') {
                    if (tos == Construct.OBJECT || tos == Construct.GHOST) {
                        ch = m_input.nextNonWSChar(false);
                        state = State.MEMBER;
                    } else {
                        state = State.VALUE;
                    }
                } else {
                    switch (tos) {
                    case ARRAY:
                        check(ch == ']', "']' or ',' expected: " + ch);
                        listener.onEndArray();
                        break;
                    case GHOST:
                        check(ch == '}', "'}' or ',' expected: " + ch);
                        break;
                    case OBJECT:
                        check(ch == '}', "'}' or ',' expected: " + ch);
                        listener.onEndObject();
                        break;
                    }
                    pop();
                    if (emptyStack()) {
                        bFinished = true;
                    }
                    // If not finished, stay in NEXT state.
                }
                break;
                
            default:
                assert false: "Missing case for state: " + state;
            }
        }
        
        // Here, we should be at EOF
        ch = m_input.nextNonWSChar(true);
        check(ch == EOF, "End of input expected: " + ch);
    }   // parse
    
    ///// Private methods

    // Push the given node onto the node stack.
    private void push(Construct construct) {
        // Make sure we don't blow the stack.
        check(m_stackPos < MAX_STACK_DEPTH, "Too many JSON nested levels (maximum=" + MAX_STACK_DEPTH + ")");
        m_stack[m_stackPos++] = construct;
    }   // push

    // Return the node currently at top of stack or null if there is none.
    private Construct tos() {
        assert m_stackPos > 0;
        return m_stack[m_stackPos - 1];
    }   // tos
    
    // Pop the stack by one and return the node removed.
    private Construct pop() {
        assert m_stackPos > 0;
        return m_stack[--m_stackPos];
    }   // pop
    
    // Return true if the stack is empty.
    private boolean emptyStack() {
        return m_stackPos == 0;
    }   // emptyStack
    
    private static void check(boolean condition, String errMsg) {
        Utils.require(condition, errMsg);
    }   // check
    
}   // class JSONAnnie
