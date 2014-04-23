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

import com.dell.doradus.olap.aggregate.AggregationResult.AggregationGroup;

public class AggregationGroupChangeCasing {

	public static void changeCasing(int level, AggregationResult result, boolean bUpper) {
		if(level == 0) changeCasing(result, bUpper);
		else for(AggregationGroup group: result.groups) {
			if(group.innerResult == null) continue;
			changeCasing(level - 1, group.innerResult, bUpper);
		}
	}

	public static void changeCasing(AggregationResult result, boolean bUpper) {
		for(AggregationGroup group: result.groups) {
			if(group.name == null) continue;
			group.name = bUpper ? group.name.toUpperCase() : group.name.toLowerCase();
		}
	}
	
}
