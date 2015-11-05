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

package com.dell.doradus.search.aggregate;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.query.Query;

public class AggregationGroup {

    public static enum  Selection  {
        Top,
        Bottom,
        First,
        Last,
        None
    }

    public static enum  SubField  {
        MINUTE,
        HOUR,
        DAY,
        MONTH,
        YEAR
    }


    // alias or name of group, excluding TOP N
    public String name;
    
    public TableDefinition tableDef;
    
    public String text;

    public Selection selection = Selection.None;
    public int selectionValue;

    //Stop words list
    public List<String> stopWords;

    //Truncate function name
    public String truncate;

    //Subfield name
    public SubField subField;

    public String timeZone;
    
    //Batch values 
    public List<Object> batch;
    
    //Batchex filters
    public List<String> batchexAliases;
    public List<Query> batchexFilters;

    //To case function name
    public String tocase; 
    
    //Exclude filter
    public List<String> exclude;

    //INCLUDE filter
    public List<String> include;

    //Global WHERE filter grouping starts with: e.g. f=WHERE(Subject:a*).Subject
    public Query filter;
    
    //List of links and associated queries
    public List<AggregationGroupItem> items; // = new ArrayList<AggregationGroupItem>();
    
    public AggregationGroup(TableDefinition tableDef) { this.tableDef = tableDef; }
    
    public FieldDefinition getLastField() {
    	return items == null || items.size() == 0 ? null : items.get(items.size() - 1).fieldDef;
    }
    
    public static List<AggregationGroup> GetAggregationGroupsList(String input, TableDefinition tableDef)  {
        return AggregationQueryBuilder.Build(input, tableDef);
    }

    public static ArrayList<ArrayList<AggregationGroup>>  GetAggregationList(String input, TableDefinition tableDef)  {
        return AggregationQueryBuilder.BuildAggregation(input, tableDef);
    }

    
    // All where queries from items
    public Query whereFilter;

    public String getPathString() {
    	String str = "";
    	if(filter != null) {
    		str += "WHERE(" + filter + ").";
    	}
    	for(int i = 0; i < items.size(); i++) {
    		if(i > 0) str += ".";
    		str += items.get(i).toString();
    	}
    	return str;
    }
    
}

