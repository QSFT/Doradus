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

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.search.parser.grammar.GrammarItem;

import java.util.ArrayList;
import java.util.Stack;

public class FieldSetQueryBuilder {

    public static FieldSetItem BuildFieldSet(String text) {

        ArrayList<GrammarItem> items = DoradusQueryBuilder.ParseFieldSet(text);
        FieldSetItem parent = new FieldSetItem("root");
        Stack<FieldSetItem> stack = new Stack<FieldSetItem>();

        stack.push(parent);

        for (int i = 0; i < items.size(); i++) {
            GrammarItem item = items.get(i);

            if (item.getType().equals("WhiteSpaces")) {
                continue;
            }

            if (item.getType().equals("semantic")) {
                continue;
            }

            if (item.getType().equals("COMMA")) {
                parent = stack.peek();
                continue;
            }

            if (item.getType().equals("LEFTPAREN")) {
                stack.push(parent);
                continue;
            }

            
            if (item.getType().equals("DOT")) {
                continue;
            }

            if (item.getType().equals("RIGHTPAREN")) {
                parent = stack.pop();
                continue;
            }

            if (item.getType().equals("BRACKET")) {
                continue;
            }

            if (item.getType().equals("LIMIT")) {
                
                try {
                    parent.limit = Integer.parseInt(item.getValue());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Cannot parse integer value " + item.getValue(), e);
                }
                continue;
            }

            if (item.getValue().equals("WHERE")) {
                ArrayList<GrammarItem> subList = new ArrayList<GrammarItem>();
                if (parent.grammarItems == null)
                    parent.grammarItems = new ArrayList<ArrayList<GrammarItem>>();
                parent.grammarItems.add(subList);

                while (true) {
                    i++;
                    GrammarItem grammarItem = items.get(i);
                    if (grammarItem.getValue().equals(SemanticNames.ENDWHERE)) {
                        break;
                    }
                    subList.add(grammarItem);
                }

                continue;
            }
            
            String fieldName = item.getValue();
            if (fieldName != null && fieldName.startsWith("_")) {
                try {
                    CommonDefs.SystemFields.valueOf(fieldName);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unknown system field " + fieldName);
                }
            }

            FieldSetItem next = GetChild(parent, fieldName);
            if (next == null)
                next = new FieldSetItem(fieldName);
            parent.children.add(next);
            parent = next;

        }

        parent = stack.pop();
        return parent;
    }

    public static FieldSetItem GetChild(FieldSetItem parent, String value) {
        //for (int i = 0; i < parent.children.size(); i++) {
        //    FieldSetItem fieldSetItem = parent.children.get(i);
        //    if (fieldSetItem.name.equals(value))
        //        return fieldSetItem;
        //}
        return null;
    }
}
