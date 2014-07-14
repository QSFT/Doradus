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

import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.query.Query;

public class AggregationMetric implements MetricExpression {
    public String sourceText;
    // function name
    public String function;

    public TableDefinition tableDef;
    
    public AggregationMetric(TableDefinition tableDef) { this.tableDef = tableDef; }
    
    // Subfield - MINUTE, HOUR, DAY, MONTH, YEAR
    public AggregationGroup.SubField subField;
    public List<AggregationGroupItem> items;
    
    //Global WHERE filter metric starts with: e.g. m=COUNT(WHERE(Attachments IS NULL).Sender.Person.Name)
    public Query filter;
    
}
