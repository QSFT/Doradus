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


public class Token implements GrammarRule {

    String name = "token";
    public GrammarRule rule;

    public Token(GrammarRule rule) {
        this.rule = rule;
    }

    public Context Match(Context context) {
        if (rule == null)
            return null;

        Context current = new Context(context.ptr, context.inputString);
        current = rule.Match(current);
        if (current != null) {
            if (context.ptr < current.ptr) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < current.items.size(); i++) {
                    GrammarItem item = current.items.get(i);
                    builder.append(item.getValue());
                }
                String lexem  = getMatchedValue(builder.toString());
                //String lexem = context.inputString.substring(context.ptr, current.ptr);
                context.items.add(new Literal(lexem, "lexem", context.ptr));
                context.ptr = current.ptr;

            }
            //TODO do we need copy  items from current.items to context.items
            return context;
        }
        return null;

    }
    
    public String getMatchedValue(String value) {
        return value;
    }

    public String Name() {
        return rule.Name();
    }
}
