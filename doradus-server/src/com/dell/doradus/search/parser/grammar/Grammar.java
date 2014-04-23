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

import java.util.Arrays;
import java.util.List;

public class Grammar {

    public static List<GrammarRule> asRule(GrammarRule... a) {
        return Arrays.asList(a);
    }

    public static GrammarRule Semantic(String name) {
        return new Semantic(name);
    }

    public static GrammarRule SetType(String name) {
        return new Semantic(name) {
            @Override
            public Context Match(Context context) {

                GrammarItem item = context.items.get(context.items.size() - 1);
                item.setType(name);
                return context;
            }
        };
    }


    public static GrammarRule SetTextValue(String name) {
        return new Semantic(name) {
            @Override
            public Context Match(Context context) {

                GrammarItem item = context.items.get(context.items.size() - 1);
                item.setValue(name);
                return context;
            }
        };
    }



    //Immediately terminates parsing and throws an exception when executed
    public static GrammarRule Error(String name) {
        return new Error(name);
    }

    public static GrammarRule Error(String name, String message) {
        return new Error(name, message);
    }

    static int fictiveRuleNumber = 0;

    public static SequenceRule Rule(GrammarRule... rules) {
        return new SequenceRule("Fictive " + fictiveRuleNumber++, rules);
    }

    public static SequenceRule Rule(String name, GrammarRule... rules) {
        return new SequenceRule(name, rules);
    }


    public static enum CharacterType {
        Letter, Digit, LetterOrDigit
    }

    public static CharacterRule Char = new CharacterRule() {

        public String Name() {
            return "Char";
        }

        public boolean Accept(char c) {
            return true;
        }
    };

    public static CharacterRule Digit = new CharacterRule() {

        public String Name() {
            return "Digit";
        }

        public boolean Accept(char c) {
            return Character.isDigit(c);
        }
    };

    public static CharacterRule HexDigit = new CharacterRule() {

        public String Name() {
            return "HexDigit";
        }

        public boolean Accept(char c) {
            return Character.digit(c, 16) != -1 ;
        }
    };


    public static CharacterRule Letter = new CharacterRule() {
        public String Name() {
            return "Letter";
        }

        public boolean Accept(char c) {
            return Character.isLetter(c);
        }
    };

    public static CharacterRule ExceptChar(char ch) {

        return  new CharacterRule(ch) {

        public String Name() {
            return "ExceptChar " + pattern;
        }

        public boolean Accept(char ch) {
            return !pattern.equals(ch ) ;
        }
        };
    }

        public static Error unexpectedToken = new Error("Unexpected token")  {
            @Override
            public Context Match(Context context) {
                message = "Unexpected token: " + context.items.get(context.items.size()-1).getValue();
                return super.Match(context);
            }
        };

    // When used in SequenceRule reports error if this rule is not matched
    public static GrammarRule MustMatchAction = new GrammarRule() {
        public String Name() {
            return "MustMatchAction";
        }

        public Context Match(Context context) {
            context.error = true;
            return context;
        }
    };


    //Use this rule for debug ; insert this rule in desired place in the grammar
    public static GrammarRule debugGrammarRule = new GrammarRule() {

        public Context Match(Context context) {
            //Set breakpoint here
            return context;
        }

        public String Name() {
            return "debugGrammarRule";
        }
    };

    // Accept any input
    public static GrammarRule emptyRule = new GrammarRule() {

        public Context Match(Context context) {
            return context;
        }

        public String Name() {
            return "NULL";
        }
    };

    public static GrammarRule WhiteSpaces = new GrammarRule() {

        public String Name() {
            return "WhiteSpaces";
        }

        public Context Match(Context context) {

            int ptr = context.ptr;
            int len = context.inputString.length();
            String source = context.inputString;
            while (ptr < len && Character.isWhitespace(source.charAt(ptr))) {
                ptr++;
            }
            if (ptr > context.ptr) {
                String lexem = context.inputString.substring(context.ptr, ptr);
                context.items.add(new Literal(lexem, "WhiteSpaces", context.ptr));

                context.ptr = context.ptr + lexem.length();
                return context;
            }

            return null;
        }
    };


    public static GrammarRule InputPointer = new GrammarRule() {

        public String Name() {
            return "InputPointer";
        }

        public Context Match(Context context) {
                context.items.add(new Literal(Name(), Name(), context.ptr));
                return context;
        }
    };


    public static GrammarRule WordLiteral = new GrammarRule() {
        String startPattern = "*?-_@#";
        //String middlePattern = "-";

        public String Name() {
            return "WordLiteral";
        }

        public Context Match(Context context) {
            int ptr = context.ptr;

            if (ptr >= context.inputString.length())
                return null;

            String value = IdentifierFromString(context,ptr, startPattern, startPattern);
            if (value == null)
                return null;
            
            context.items.add(new Literal(value, "lexem", ptr));
            return context;
        }

        public String IdentifierFromString(Context cntx, int pos, String starts, String middle)
        {
            String source = cntx.inputString;
            char c = source.charAt(pos);
            if (Character.isLetterOrDigit(c) || starts.indexOf(c) != -1) {
                int startPos = pos;
                int len = source.length();
                pos++;
                while (pos < len)
                {
                    c = source.charAt(pos);
                    if (!(Character.isLetterOrDigit(c) || middle.indexOf(c) != -1)) {
                        cntx.ptr = pos;
                        return source.substring(startPos, pos);
                    }
                    pos++;

                }
                cntx.ptr = pos;
                return source.substring(startPos);

            }
            return null;
        }
    };

    
    public static GrammarRule DropLexem = new GrammarRule() {
        public String Name() {
            return "DropLexem";
        }

        public Context Match(Context context) {
            context.items.remove(context.items.size()-1);
            return context;
        }
    };

}
