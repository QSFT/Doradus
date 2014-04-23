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

package com.dell.doradus.olap.aggregate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.fieldanalyzer.SimpleTextAnalyzer;
import com.dell.doradus.olap.aggregate.AggregationResult.AggregationGroup;

public class AggregationGroupTokenizer {

	public static void tokenizeValues(int level, AggregationResult result, MetricCollectorSet mcs) {
		if(level == 0) tokenizeValues(result, mcs);
		else for(AggregationGroup group: result.groups) {
			if(group.innerResult == null) continue;
			tokenizeValues(level - 1, group.innerResult, mcs);
		}
	}

	public static void tokenizeValues(AggregationResult result, MetricCollectorSet mcs) {
		Map<String, AggregationGroup> map = new HashMap<String, AggregationGroup>();
		for(AggregationGroup group: result.groups) {
			if(group.name == null) {
				map.put(null, group);
				continue;
			}
			String[] tokens = SimpleTextAnalyzer.instance().tokenize(group.name);
			for(String token: tokens) {
				AggregationGroup newgroup = map.get(token);
				if(newgroup == null) {
					newgroup = new AggregationGroup();
					newgroup.id = token;
					newgroup.name = token;
					newgroup.metricSet = mcs.get(-1);
					map.put(token, newgroup);
				}
				newgroup.merge(group);
			}
		}
		result.groups.clear();
		result.groups.addAll(map.values());
		Collections.sort(result.groups);
	}
	
}
