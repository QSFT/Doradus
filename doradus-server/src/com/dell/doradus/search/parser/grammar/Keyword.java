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


public class Keyword implements GrammarRule {

    String value;
    GrammarRule rule;
    boolean caseInsensetive;

    public Keyword(String value, GrammarRule rule) {
        this(value, rule, false);
    }

    public Keyword(String value, GrammarRule rule, boolean caseeinsensetive) {
        this.rule = rule;
        this.caseInsensetive = caseeinsensetive;
        //if (caseeinsensetive)
        //    this.value = value.toLowerCase();
        //else
        this.value = value;
    }
    
    
    @Override
    public Context Match(Context context) {

        Context current = new Context(context.ptr, context.inputString);
        current = rule.Match(current);
        if (current != null) {
            if (current.items.size() == 1) {
                GrammarItem item = current.items.get(0);
                String matchedValue = caseInsensetive? item.getValue().toLowerCase():item.getValue() ;
                String original = this.value;
                if (caseInsensetive)
                    original = value.toLowerCase();


                if (original.equals(matchedValue)) {
                    item = new Literal(value, "token", context.ptr);
                    context.items.add(item);
                    context.ptr = current.ptr;
                    return context;
                }
            }
        }
        return null;
    }

    @Override
    public String Name() {
        return "\"" + value + "\"";
    }
}
