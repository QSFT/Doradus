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
import java.util.List;


public class Context {

    public int ptr;
    public String inputString;
    public ArrayList<GrammarItem> items;
    public boolean debug = false;
    public boolean error = false;

    public Context(String input) {
        this(0, input, new ArrayList<GrammarItem>());

    }

    public Context(int ptr, String input) {
        this(ptr, input, new ArrayList<GrammarItem>());

    }

    public Context(int ptr, String input, List<GrammarItem> items) {
        this.ptr = ptr;
        inputString = input;

        this.items = new ArrayList<GrammarItem>(items);
        //for (int idx = 0; idx < items.size(); ++idx) {
        //    this.items.add(items.get(idx));
        //}
    }

    public void LogInfo(String s) {
        LogInfo(s, debug);
    }

    public void LogInfo(String s, boolean show) {
        if (show) {
            System.out.println(s);
        }
    }
}
