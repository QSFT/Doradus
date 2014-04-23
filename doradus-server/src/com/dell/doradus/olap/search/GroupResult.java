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

package com.dell.doradus.olap.search;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.UNode;

public class GroupResult {
	public boolean isSortByCount = false;
	public int totalCount;
	public int groupsCount;
	public List<GroupCount> groups = new ArrayList<GroupCount>();

	public GroupResult() {}
	@Override
	public String toString() {
		String str = "" + totalCount + ", [ ";
		for(GroupCount g : groups) str += g.toString() + " ";
		str += "]";
		return str;
	}
	
	public UNode toUNode() {
	    UNode result = UNode.createMapNode("results");
	    result.addValueNode("total", "" + totalCount);
	    result.addValueNode("groupsCount", "" + groupsCount);
	    UNode groupsNode = result.addArrayNode("groups");
	    for (GroupCount group : groups) {
	        UNode groupNode = groupsNode.addMapNode("group");
	        groupNode.addValueNode("name", group.name);
	        groupNode.addValueNode("count", "" + group.count);
	    }
	    return result;
	}
	
}
