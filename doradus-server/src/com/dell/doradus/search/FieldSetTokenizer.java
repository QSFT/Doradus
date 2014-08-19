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

package com.dell.doradus.search;


import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.parser.*;

import java.util.ArrayList;
import java.util.List;

class FieldSetTokenizer {
    public String text;
    public int position;
    public StringBuilder currentField = new StringBuilder();
    public FieldSet currentSet;

    public FieldSetTokenizer(FieldSet set, String text) {
        this.text = text;
        currentSet = set;
        position = 0;
        currentField.setLength(0);
    }


    private static List<FieldDefinition> GetFieldList(List<FieldDefinition> list, FieldDefinition fieldDef) {
        if (list == null)
            list = new ArrayList<FieldDefinition>();

        if (fieldDef != null && fieldDef.isGroupField()) {
            for (FieldDefinition nested : fieldDef.getNestedFields()) {
                list = GetFieldList(list, nested);
            }
        } else {
            list.add(fieldDef);
        }
        return list;
    }

    private void Parse() {
        FieldSetItem root = FieldSetQueryBuilder.BuildFieldSet(text);
        for (int i = 0; i < root.children.size(); i++) {
            FieldSetItem nextItem = root.children.get(i);
            CreateItemSet(nextItem, currentSet.tableDef, currentSet);
        }
    }

    public static void CreateItemSet(FieldSetItem item, TableDefinition def, FieldSet set) {

        if (def != null) {
            FieldDefinition fieldDef = def.getFieldDef(item.name);
            if (fieldDef != null) {
                List<FieldDefinition> list = GetFieldList(null, fieldDef);
                for (int i = 0; i < list.size(); i++) {
                    FieldDefinition fieldDefinition = list.get(i);
                    FieldSet current = set;
                    if (!fieldDefinition.isLinkField() && item.children.size() > 0)
                        throw new IllegalArgumentException(fieldDefinition + " is not a link");

                    if (fieldDefinition.isLinkField()) {
                        //if (set.LinkFields.containsKey(fieldDefinition.getName()))
                        //    current = set.LinkFields.get(fieldDefinition.getName());
                        //else   {
                            current = new FieldSet(def.getLinkExtentTableDef(fieldDefinition));
                            current.limit = item.limit;
                            current.alias= item.alias;
                            if (item.grammarItems != null) {
                                    for (int j = 0; j < item.grammarItems.size(); j++) {
                                        current.filter =  AggregationQueryBuilder.CompileQuery(current.tableDef,current.filter, item.grammarItems.get(j)  );
                                    }
                             }
                        //}

                        //set.LinkFields.put(fieldDefinition.getName(), current);
                            set.addLink(fieldDefinition.getName(), current);

                        for (int k = 0; k < item.children.size(); k++) {
                            FieldSetItem nextItem = item.children.get(k);
                            CreateItemSet(nextItem, def.getLinkExtentTableDef(fieldDefinition), current);
                        }
                    } else {
                        if (!set.ScalarFields.contains(fieldDefinition.getName())) {
                            set.ScalarFields.add(fieldDefinition.getName());
                            if (item.alias != null)
                                set.ScalarFieldAliases.put(fieldDefinition.getName(), item.alias);
                        }
                    }
                }
            } else {
                if (item.children.size() > 0)
                    throw new IllegalArgumentException(item.name + " is not a link");

                if (!set.ScalarFields.contains(item.name))   {
                	set.ScalarFields.add(item.name);
                    if (item.alias != null)
                        set.ScalarFieldAliases.put(item.name, item.alias);
                }
            }
        } else {
            for (int i = 0; i < item.children.size(); i++) {
                FieldSetItem nextItem = item.children.get(i);
                set.ScalarFields.add(nextItem.name);
                if (nextItem.alias != null)
                    set.ScalarFieldAliases.put(item.name, item.alias);
                //Check size ?
            }
        }
    }

    public void ProcessField() {
        Parse();
    }

}

