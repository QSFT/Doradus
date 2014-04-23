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

import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;

import java.util.List;

public class Statistic {

    public static AggregationMetric GetStatisticMetric(String input, TableDefinition tableDef)  {
        return AggregationQueryBuilder.BuildStatisticMetric(input, tableDef);
    }

    public static List<AggregationGroup> GetStatistic(String input, TableDefinition tableDef)  {
        return AggregationQueryBuilder.BuildStatistic(input, tableDef);
    }

     public static List<String> GetMetricFields(AggregationMetric metric) {
         return AggregationQueryBuilder.GetMetricFields(metric);
     }

    public static List<String> GetGroupFields(List<AggregationGroup> list) {
        return AggregationQueryBuilder.GetMetricFields(null);
    }

    public static Statistic.StatisticParameter GetStatisticParameters(String input) {
        return AggregationQueryBuilder.BuildStatisticParameters(input);
    }
    
    public static void ValidateStatistic(StatisticDefinition stat, TableDefinition tableDef) 
    {
    	try
    	{
    		String metricParams = stat.getMetricParam();
    		if(metricParams != null && !metricParams.isEmpty())
    		{
    			Statistic.GetStatisticMetric(metricParams, tableDef);
    		}
    		String groupParams = stat.getGroupParam();
    		if(groupParams != null && !groupParams.isEmpty())
    		{
    			Statistic.GetStatistic(groupParams, tableDef);
    		}
    		String queryParams = stat.getQueryParam();
    		if(queryParams!= null && ! queryParams.isEmpty())
    		{
    			DoradusQueryBuilder.Build(queryParams, tableDef);
    		}
    	}
    	catch(Exception e)
    	{
    		throw new IllegalArgumentException(e.getMessage());
    	}
	}

	public static class StatisticParameter  {
        public int level;
        public String minValue;
        public String maxValue;
    }
}
