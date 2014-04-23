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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SequenceRule implements GrammarRule {

    String name;
    public List<GrammarRule> body = new ArrayList<GrammarRule>();


    public SequenceRule(String name, GrammarRule... rules) {
        this.name = name;
        this.body = Arrays.asList(rules);
    }

    public SequenceRule(GrammarRule rule) {
        this.body = new ArrayList<GrammarRule>();
        body.add(rule);

    }

    public List<GrammarRule> getBody() {
        return body;
    }

    @Override
    public String Name() {
        return "<" + name + ">";
    }

    public Context Match(Context context) {

        Context current = new Context(context.ptr, context.inputString, context.items);
        boolean reportError = false;
        for (int i = 0; i < body.size(); i++) {
            //current.LogInfo(" (try " + Name() + " seq=" + i + " rule=" + body.get(i).Name() + "  ptr= " + current.ptr + " ) -->" + current.inputString.substring(current.ptr));
            Context next = body.get(i).Match(current);
            if (next == null) {
                //String s1 = name + " (seq " + i + "(return null)";
                //context.LogInfo(s1);
                if (reportError) {
                    throw new GrammarException("Unexpected character (" + name + ")", current);
                }
                return null;
            } else {
                if (next.error) {
                    reportError = next.error;
                    next.error = false;
                }

            }
            current = next;
        }

        //current.LogInfo(name + " (seq  return " + current.ptr + " ) -->" + current.inputString.substring(current.ptr));
        return current;

    }

}
