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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.dell.doradus.common.UNode;
import com.dell.doradus.search.util.HeapSort;

public class AggregationResult {
	public int documentsCount;
	public int groupsCount;
	
	public AggregationGroup summary;
	public List<AggregationGroup> groups = new ArrayList<AggregationGroup>();

	public UNode toUNode() {
	    UNode result = UNode.createMapNode("results");
	    result.addValueNode("documentsCount", "" + documentsCount, true);
	    result.addValueNode("groupsCount", "" + groupsCount, true);
	    
	    if(summary != null) {
		    UNode groupsNode = result.addArrayNode("summary");
		    groupsNode.addChildNode(summary.toUNode());
	    }
	    
	    UNode groupsNode = result.addArrayNode("groups");
	    for (AggregationGroup group : groups) {
	    	groupsNode.addChildNode(group.toUNode());
	    }
	    return result;
	}
	
	@Override public String toString() {
		String str = toUNode().toXML();
		if(str.length() > 10000) str = str.substring(0, 10000) + "...";
		return str;
	}
	
	public static class AggregationGroup implements Comparable<AggregationGroup> {
		// internal ID used for sorting
		public Object id;
		// display name for showing to the user
		public String name;
		// metrics value for the group
		public MetricValueSet metricSet;
		// inner result if this group is not a leaf node
		public AggregationResult innerResult;
		
		public void merge(AggregationGroup other) {
			metricSet.add(other.metricSet);
			
			if(innerResult == null) {
				if(other.innerResult != null) innerResult = other.innerResult;
			} else {
				if(other.innerResult != null) {
					AggregationResult[] rs = new AggregationResult[2];
					rs[0] = innerResult;
					rs[1] = other.innerResult;
					innerResult = AggregationResult.merge(rs, 0);
				}
			}
		}
		
		/**
		 * helper method to get group's count if first metric is integer
		 * @return
		 */
		public long getCount() {
			String val = metricSet.values[0].toString();
			if(val == null) return 0;
			try {
				return Long.parseLong(val);
			}catch(NumberFormatException e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override public int compareTo(AggregationGroup other) {
			if(id == null && other.id == null) return 0;
			else if(id == null) return -1;
			else if(other.id == null) return 1;
			else return ((Comparable)id).compareTo(other.id);
		}
		
		@Override public boolean equals(Object obj) {
			AggregationGroup other = (AggregationGroup)obj;
			if(id == null && other.id == null) return true;
			else if(id == null) return false;
			else if(other.id == null) return false;
			else return id.equals(other.id);
		}
		
		@Override public int hashCode() {
			if(id == null) return 0;
			else return id.hashCode();
		};
		
		public UNode toUNode() {
		    UNode result = UNode.createMapNode("group");
		    if(name != null) result.addValueNode("name", name, true);
		    for(int i = 0; i < metricSet.values.length; i++) {
		    	String value = metricSet.values[i].toString();
		    	if(value == null) value = "";
		    	result.addValueNode("metric_" + i, value, true);
		    }
		    if(innerResult != null) {
		    	result.addChildNode(innerResult.toUNode());
			    //for (AggregationGroup group : innerResult.groups) {
			    //	result.addChildNode(group.toUNode());
			    //}
		    }
		    return result;
		}
		
		@Override public String toString() {
			return toUNode().toXML();
		}
	}
	
	public static AggregationResult merge(AggregationResult[] results, int top) {
		AggregationResult result = new AggregationResult();
		HeapSort<AggregationGroup> sorter = new HeapSort<AggregationGroup>();
		for(AggregationResult r : results) {
			result.documentsCount += r.documentsCount;
			Collections.sort(r.groups);
			sorter.add(r.groups);
			if(result.summary == null) result.summary = r.summary;
			else if(r.summary != null) result.summary.merge(r.summary);
		}
		AggregationGroup current = null;
		for(AggregationGroup group : sorter) {
			if(current == null) current = group;
			else if(current.compareTo(group) == 0) current.merge(group);
			else {
				result.groups.add(current);
				current = group;
			}
		}
		if(current != null) result.groups.add(current);
		
		result.groupsCount = result.groups.size();

		applyLimit(result, top);
		
		return result;
	}

	public static void applyLimits(AggregationResult result, AggregationRequest request, int level) {
		if(result == null) return;
		applyLimit(result, request.getTop(level));
		for(AggregationGroup group : result.groups) {
			applyLimits(group.innerResult, request, level + 1);
		}
	}
	
	private static void applyLimit(AggregationResult result, int top) {
		if(top > 0) {
			Collections.sort(result.groups, new Comparator<AggregationGroup>(){
				@Override public int compare(AggregationGroup x, AggregationGroup y) {
					return y.metricSet.compareTo(x.metricSet);
				}});
			if(result.groups.size() > top) result.groups = result.groups.subList(0, top);
		}
		else if(top < 0) {
			Collections.sort(result.groups, new Comparator<AggregationGroup>(){
				@Override public int compare(AggregationGroup x, AggregationGroup y) {
					return x.metricSet.compareTo(y.metricSet);
				}});
			if(result.groups.size() > -top) result.groups = result.groups.subList(0, -top);
		}
	}
	
}
