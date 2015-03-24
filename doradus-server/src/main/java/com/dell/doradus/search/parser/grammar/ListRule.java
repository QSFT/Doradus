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

public class ListRule implements GrammarRule {

    String name;
    public GrammarRule listValue;
    public GrammarRule listDelimiter;


    public ListRule(String name, GrammarRule value, GrammarRule delimiter) {
        this.name = name;
        listValue = value;
        listDelimiter = delimiter;
    }

    @Override
    public String Name() {
        return "<" + name + ">";
    }

    public Context Match(Context context) {

        Context next = listValue.Match(new Context(context.ptr, context.inputString, context.items));
        while (next != null) {
            //check for list continuation token
            Context continueContext = listDelimiter.Match(next);
            if (continueContext != null) {
                next = listValue.Match(continueContext);
                if (next == null)
                    throw new GrammarException("Unexpected character:'" +  GetCurrentChar(continueContext) + "' , position=" + (continueContext.ptr + 1), continueContext );
            } else {
                return next;
            }
        }
        return next;
    }

    //TODO Create Utils class for diagnostics and move this code
    private static String  GetCurrentChar( Context context) {
        if (context.ptr >= context.inputString.length())
            return "EOL";
        return "" + context.inputString.charAt(context.ptr);
    }
}
