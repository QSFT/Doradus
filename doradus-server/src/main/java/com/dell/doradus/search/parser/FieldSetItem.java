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

package com.dell.doradus.search.parser;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.search.parser.grammar.GrammarItem;

public class FieldSetItem {

    FieldSetItem(String name) {
        this.name = name;
    }

    public String name;
    public List<FieldSetItem> children = new ArrayList<FieldSetItem>();
    public int limit =-1;
    public ArrayList<ArrayList<GrammarItem>> grammarItems;
    public String alias;
}
