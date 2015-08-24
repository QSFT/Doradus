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

package com.dell.doradus.search.query;

import com.dell.doradus.search.aggregate.AggregationGroup;


/**
 * Query that filters a document based on whether
 * any of its linked documents satisfies the query specified 
 */
public class EqualsQuery implements Query {
    public AggregationGroup group1;
    public AggregationGroup group2;
	
	public EqualsQuery() { }

    public EqualsQuery(AggregationGroup group1, AggregationGroup group2) {
        this.group1 = group1;
        this.group2 = group2;
    }
	
	@Override
	public String toString() {
		return String.format("EQUALS(%s,%s)", group1.toString(), group2.toString());
	}
	
}
