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

public class SwitchRule implements GrammarRule {

    public static String Longest = "Longest";
    public static String First = "First";
    public static String Optional = "Optional";


    public boolean optionalMode = false;

    public void setMode(String mode) {
        this.mode = mode;
    }

    String mode;

    String name;
    public List<GrammarRule> body = new ArrayList<GrammarRule>();

    public List<GrammarRule> getBody() {
        return body;
    }

    @Override
    public String Name() {
        return "<" + name + ">";
    }

    public SwitchRule(String mode, String name, GrammarRule... rule) {
        this.name = name;
        this.mode = mode;
        body = Arrays.asList(rule);
    }

    public SwitchRule(String name, GrammarRule... rule) {
        this(Longest, name, rule);
    }

    public Context Match(Context context) {

        Context maxMatched = null;


        for (int i = 0; i < body.size(); i++) {
            Context current = new Context(context.ptr, context.inputString, context.items);
            //context.LogInfo(name + " try match=" + body.get(i) + "  (" + i + "   " + current.ptr + ") -->" + current.inputString.substring(current.ptr));

            Context result = body.get(i).Match(current);
            if (result != null) {

                if (mode.equals(First))
                    return result;

                if (maxMatched == null) {
                    maxMatched = result;
                } else {
                    if (result.ptr >= maxMatched.ptr)
                        maxMatched = result;
                }
            }
        }

        /*
        if (maxMatched == null) {
            String s = name + "--> return null";
            context.LogInfo(s);

        } else {
            context.LogInfo(name + "  return " + maxMatched.ptr + " --> " + maxMatched.inputString.substring(maxMatched.ptr));
        }
        */

        if (maxMatched == null && optionalMode)
            return context;

        return maxMatched;

    }

}
