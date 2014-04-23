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

package com.dell.doradus.search.parser.grammar;

public class StringToken implements GrammarRule {

    char delimeter;
    String name;

    public String Name() {
        return name;
    }

    public StringToken(String name, char delimeter) {
        this.delimeter = delimeter;
        this.name = name;
    }

    public Context Match(Context context) {
        int ptr = context.ptr;

        if (ptr >= context.inputString.length())
            return null;

        if (context.inputString.charAt(ptr) != delimeter)
            return null;

        String value = LiteralFromString(context,ptr+1, delimeter);
        context.items.add(new Literal(value, "string", ptr));
        return context;
    }

    public static String IdentifierFromString(Context cntx, int pos, String starts, String middle)
    {

        String source = cntx.inputString;
        char c = source.charAt(pos);
        if (Character.isLetterOrDigit(c) || starts.indexOf(c) != -1) {
            int startPos = pos;
            int len = source.length();
            while (pos < len)
            {
               pos++;
               c = source.charAt(pos);
               if (!(Character.isLetterOrDigit(c) || middle.indexOf(c) != -1)) {
                   break;
               }

            }
            cntx.ptr = pos;
            return source.substring(startPos, pos-1);

        }
        return null;
    }


    public static String LiteralFromString(Context cntx, int pos, char delimeter)
    {
        String source = cntx.inputString;
        int len = source.length();
        StringBuilder builder = new StringBuilder();

        while (pos < len)
        {

            char c = source.charAt(pos);

            if (c== delimeter) {
                cntx.ptr = pos+1;
                //return source.substring(startpos, cntx.ptr-1 );
                return builder.toString();
            }
            
            if (c == '\\')
            {
                pos++;
                if (pos >= len) throw new IllegalArgumentException("Missing escape sequence");
                switch (source.charAt(pos))
                {
                    // --- Simple character escapes
                    case '\'': c = '\''; break;
                    case '\"': c = '\"'; break;
                    case '\\': c = '\\'; break;
                    case 'b': c = '\b'; break;
                    case 'f': c = '\f'; break;
                    case 'n': c = '\n'; break;
                    case 'r': c = '\r'; break;
                    case 't': c = '\t'; break;
                    case 'u':
                        pos++;
                        if (pos + 3 >= len)
                            throw new IllegalArgumentException("Unrecognized escape sequence");
                            c = (char) Integer.parseInt(source.substring(pos, pos+4),16);
                            pos += 3;

                        break;

                    default:
                        throw new IllegalArgumentException("Unrecognized escape sequence");
                }
            }
            builder.append(c);
            pos++;
        }
        throw new IllegalArgumentException("There is no closing character " + delimeter);
    }



}
