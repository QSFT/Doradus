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

package com.dell.doradus.service.statistic;

import java.io.IOException;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.aggregate.Aggregate;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Performs data aggregation for a given StatisticRunner task.
 */
public class Mapper {
	public static void map(StatisticRunner task)
    {
		ApplicationDefinition app = SchemaService.instance().getApplication(task.getAppName());
		StatisticDefinition definition = task.getStatDefinition();
		TableDefinition tableDef = app.getTableDef(definition.getTableName());
		
        Aggregate aggregate = new Aggregate(tableDef);
        aggregate.parseParameters(definition.getMetricParam(), definition.getQueryParam(), definition.getGroupParam());
        
        // Attempt to execute the query.
        try {
        	aggregate.execute();
            task.setResult(aggregate.getStatisticResult(definition.getStatName()));
        } catch (IOException e) {
        	throw new RuntimeException("Statistic aggregation failed");
        }
    }	// map

}	// Mapper
