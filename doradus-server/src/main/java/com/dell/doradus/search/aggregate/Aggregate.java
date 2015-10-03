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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.AggregateResult.AggGroupSet;
import com.dell.doradus.common.AggregateResult.IAggGroupList;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.search.QueryExecutor;
import com.dell.doradus.search.aggregate.AggregationGroup.Selection;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.util.LRUCache;
import com.dell.doradus.utilities.Timer;
import com.dell.doradus.utilities.TimerGroup;

public class Aggregate {
    public final static String GROUPSEPARATOR = "\u0000";
    
    // Aggregate options
	private boolean separateSearchTiming;

	static Logger log = LoggerFactory.getLogger(Aggregate.class.getSimpleName());
	TimerGroup timers = new TimerGroup(Aggregate.class.getSimpleName() + ".timing");

	String m_mParamValue;
	String m_query;
    String m_fParamValue;

    MetricPath[] m_metricPaths;
    ArrayList<ArrayList<AggregationGroup>> m_groups;
    GroupSetEntry[] m_groupSet;
    boolean m_isComposite;
	private boolean m_showGroupSets;
	private long m_totalObjects = -1; // -1 == not set

	TableDefinition m_tableDef;

	int m_indent = 0;

	private boolean m_l2rEnable;
	
    public static class StatisticResult
    {
        public final static String COUNTKEY = "COUNT";
        public final static String SUMKEY = "SUM";
        public final static String VALUEKEY = "value";
        public final static String SUMMARY = "\uefff" + "summary";
        public final static String METRICKEY = "metric";
        public final static String KEYSEPARATOR = "/";
        public final static String AVERAGESEPARATOR = ".";
        public String fieldName;
        List<AbstractMap.SimpleEntry<String, Object>> values = new ArrayList<AbstractMap.SimpleEntry<String, Object>>();
        //
        public String getFieldName()
        {
            return fieldName;
        }
        public void setFieldName(String fieldName)
        {
            this.fieldName = fieldName;
        }
        public Iterable<AbstractMap.SimpleEntry<String, Object>> getValues()
        {
            return values;
        }
        public void addValue(AbstractMap.SimpleEntry<String, Object> value)
        {
            values.add(value);
        }
    }
    
    public static class AverageStatistic extends StatisticResult
    {
        String additionalField;
        List<AbstractMap.SimpleEntry<String, Object>> additionalValues = new ArrayList<AbstractMap.SimpleEntry<String, Object>>();
        //
        public String getCountFieldName()
        {
            return fieldName;
        }
        public String getSumFieldName()
        {
            return additionalField;
        }
        public void setSumFieldName(String sumFieldName)
        {
            additionalField = sumFieldName;
        }
        public Iterable<AbstractMap.SimpleEntry<String, Object>> getCountValues()
        {
            return values;
        }
        public Iterable<AbstractMap.SimpleEntry<String, Object>> getSumValues()
        {
            return additionalValues;
        }
        public void addSumValue(AbstractMap.SimpleEntry<String, Object> value)
        {
            additionalValues.add(value);
        }
    }

	public Aggregate(TableDefinition tableDef) {
		assert tableDef != null;
		m_tableDef = tableDef;
		m_groupSet = new GroupSetEntry[0];
	    separateSearchTiming = ServerParams.instance().getModuleParamBoolean("DoradusServer", "aggr_separate_search");
	    m_l2rEnable = ServerParams.instance().getModuleParamBoolean("DoradusServer", "l2r_enable");
	}

	// Executes the request and returns the total group.
	// The total group may contain subgroups
	@SuppressWarnings("unchecked")
	public void execute() {
		Utils.require(m_metricPaths != null && m_metricPaths.length != 0, "Metric ('m') parameter is required");
        List<String> allfieldNames = new ArrayList<String>();
		for (GroupSetEntry groupSetEntry : m_groupSet) {
    		for (GroupPath groupPath : groupSetEntry.m_groupPaths){
    			preserveGroupLink(groupPath, true);
    			groupSetEntry.m_metricPath.addGroupPath(groupPath);
    		}
    		preserveGroupLink(groupSetEntry.m_metricPath, false);		
   		    allfieldNames.addAll(groupSetEntry.m_metricPath.fieldNames);
            if (groupSetEntry.m_isComposite) {
                groupSetEntry.m_compositeGroup = groupSetEntry.m_totalGroup.createSubgroup(Group.COMPOSITE_GROUP_NAME);
            }
		}
        final List<String> fieldNames = new ArrayList<String>(new HashSet<String>(allfieldNames)); // remove duplicates

		String queryText = m_query;
		if (queryText == null) queryText = "*";
		
		ArrayList<Integer> iGroupSet2Skip = new ArrayList<Integer>();
		if (queryText.equals("*")) { 												// all items
		    for (int i = 0; i < m_groupSet.length; i++) {
		        if ((m_groupSet[i].m_groupPathsParam == null) && 					// no groups
		        (m_groupSet[i].m_metricPath.branches.size() == 0) &&            	// no branches
		        (m_groupSet[i].m_metricPath.fieldType == FieldType.TIMESTAMP)) {	// TIMESTAMP field
			        Date date = null;
			        if (m_groupSet[i].m_metricPath.function.equals("MAX")) {
		                date = MaxMinHelper.getMaxDate(m_tableDef, m_groupSet[i].m_metricPath.name);
			        } else if (m_groupSet[i].m_metricPath.function.equals("MIN")) {
			            date = MaxMinHelper.getMinDate(m_tableDef, m_groupSet[i].m_metricPath.name);
			        }
			        if (date != null) {
			            m_groupSet[i].m_totalGroup.update(Utils.formatDateUTC(date.getTime()));
			            iGroupSet2Skip.add(i);
				    }
			    }			
			}
		}
		if (iGroupSet2Skip.size() == m_groupSet.length)
			return;
				
		Timer searchTimer = new Timer();
		timers.start("Search");

		QueryExecutor executor = new QueryExecutor(m_tableDef); 
		executor.setL2rEnabled(m_l2rEnable);
		Iterable<ObjectID> hits = executor.search(queryText);
//		ArrayList<ObjectID> hhh = new ArrayList<>();
//		for (ObjectID hit : hits) {
//			String id = hit.toString();
//			hhh.add(IDHelper.createID(id));
//		}

		if (separateSearchTiming) {
			ArrayList<ObjectID> hitList = new ArrayList<ObjectID>();
			for(ObjectID id : hits) hitList.add(id);
			hits = hitList;
			timers.stop("Search");
			log.debug("Search '{}' took {}", new Object[] { queryText, searchTimer });
		}

        timers.start("Aggregating");

		DBEntitySequenceFactory factory = new DBEntitySequenceFactory();
		EntitySequence collection = factory.getSequence(m_tableDef, hits, fieldNames);

        List<Set<String>[]> groupKeysSet = new ArrayList<Set<String>[]>();
        for (GroupSetEntry groupSetEntry : m_groupSet) {
            Set<String>[] groupKeys = new HashSet[groupSetEntry.m_groupPaths.length];
            for (int i = 0; i < groupKeys.length; i ++){
                groupKeys[i] = new HashSet<String>();
            }
            groupKeysSet.add(groupKeys);
        }

        m_totalObjects = 0;
		for (Entity obj : collection) {
		    m_totalObjects++;
		    for (int i = 0; i < m_groupSet.length; i++) {
                if (iGroupSet2Skip.contains(i))
                	continue;
		    	process(obj, m_groupSet[i].m_metricPath, m_groupSet[i], groupKeysSet.get(i));
		    }
		}
		
		// task #32,752 - include metrics for empty batches
		for (GroupSetEntry groupSetEntry : m_groupSet) {
	        boolean useNullGroup = !groupSetEntry.m_metricPath.function.equals("COUNT");
            addEmptyGroups(groupSetEntry.m_totalGroup, groupSetEntry.m_groupPaths, 0, useNullGroup);
            
            if (groupSetEntry.m_isComposite) {
            	addEmptyGroups(groupSetEntry.m_compositeGroup, groupSetEntry.m_groupPaths, 1, useNullGroup);
            }
		}

        // When a multi-metric aggregate query ...
        if (m_metricPaths.length > 1) { 
            if (m_groups != null) {
                int cMetrics = m_metricPaths.length;
                int cGroups = m_groups.size();
                for (int ig = 0; ig < cGroups; ig++) {
                    GroupSetEntry groupSetEntry = m_groupSet[ig];
                    // uses the TOP or BOTTOM function in the outer grouping field for the first metric function ...
                    if ((groupSetEntry.m_groupPaths[0].groupOutputParameters.function == Selection.Top) ||
                        (groupSetEntry.m_groupPaths[0].groupOutputParameters.function == Selection.Bottom)) {
                        List<Group> definedgroups = new ArrayList<Group>();
                        for (int im = 1; im < cMetrics; im++) {
                                definedgroups.add(m_groupSet[im * cGroups + ig].m_totalGroup);
                        }
                        // the outer and inner groups are selected on this metric function.
                        defineGroupsSelection(m_groupSet[ig].m_totalGroup, m_groupSet[ig].m_groupPaths, 0, definedgroups);
                    }
                }
            }
        }

        factory.timers.log();

		timers.stop("Aggregating");
		timers.log("Aggregate '%s'", m_mParamValue);
	}
	
	private void preserveGroupLink(PathEntry entry, boolean preserve)
	{
		if (entry.branches.size() == 0) {
			 if (entry.name.startsWith(PathEntry.ANY)) {
				 if (preserve)
					 entry.name = entry.name + entry.groupIndex;
				 else // restore
					 entry.name = PathEntry.ANY;
			 }
		}
		else {
			for (PathEntry child  : entry.branches) {
				preserveGroupLink(child, preserve);
			}			
		}
	}
	
	private void process(Entity obj, PathEntry entry, GroupSetEntry groupSetEntry, Set<String>[] groupKeys) {

		for(int groupIndex : entry.groupIndexes){
		    groupKeys[groupIndex].clear();
		}

		collectGroupValues(obj, entry, groupSetEntry, groupKeys);

		for(int groupIndex : entry.groupIndexes) {
			if (groupKeys[groupIndex].size()==0) return;
		}

		if(entry.query != null) {
			if(!entry.checkCondition(obj)) {
				return;
			}
		}
		if(entry.isLink) {
			for(Entity linkedObject : obj.getLinkedEntities(entry.name, entry.fieldNames)) {
				process(linkedObject, entry.branches.get(0), groupSetEntry, groupKeys);
			}
		}
		else {
			if(entry.name == PathEntry.ANY) {
				updateMetric(obj.id().toString(), groupSetEntry, groupKeys);
			}
			else {
				String value = obj.get(entry.name);
				String[] values = value == null ? new String[] { value } : value.split(CommonDefs.MV_SCALAR_SEP_CHAR);
				for (String collectionValue : values) {
					for(Integer groupIndex: entry.matchingGroupIndexes) {
					    groupKeys[groupIndex].clear();
					    groupSetEntry.m_groupPaths[groupIndex].addValueKeys(groupKeys[groupIndex], collectionValue);
					}
					updateMetric(collectionValue, groupSetEntry, groupKeys);
				}
			}
		}
	}

	// Collect all values from all object found in the path subtree
	private void collectGroupValues(Entity obj, PathEntry entry, GroupSetEntry groupSetEntry, Set<String>[] groupKeys) {
		if(entry.query != null) {
			timers.start("Where", entry.queryText);
			boolean result = entry.checkCondition(obj);
			timers.stop("Where", entry.queryText, result ? 1 : 0);
			if (!result) return;
		}

		for(PathEntry field : entry.leafBranches) {
			String fieldName = field.name;
			int groupIndex = field.groupIndex;
			if(fieldName == PathEntry.ANY) {
			    groupSetEntry.m_groupPaths[groupIndex].addValueKeys(groupKeys[groupIndex], obj.id().toString());
			}
			else {
				String value = obj.get(fieldName);
				if(value == null || value.indexOf(CommonDefs.MV_SCALAR_SEP_CHAR) == -1) {
				    groupSetEntry.m_groupPaths[groupIndex].addValueKeys(groupKeys[groupIndex], value);
				}
				else {
					String[] values = value.split(CommonDefs.MV_SCALAR_SEP_CHAR);
					for(String collectionValue : values) {
					    groupSetEntry.m_groupPaths[groupIndex].addValueKeys(groupKeys[groupIndex], collectionValue);
					}
				}
			}
		}
        for(PathEntry child : entry.linkBranches) {
			boolean hasLinkedEntities = false;
            if(child.nestedLinks == null || child.nestedLinks.size() == 0) {
                for(Entity linkedObj : obj.getLinkedEntities(child.name, child.fieldNames)) {
    				hasLinkedEntities = true;
                    collectGroupValues(linkedObj, child, groupSetEntry, groupKeys);
                }
            }
            else {
                for(LinkInfo linkInfo : child.nestedLinks) {
                    for(Entity linkedObj : obj.getLinkedEntities(linkInfo.name, child.fieldNames)) {
        				hasLinkedEntities = true;
                        collectGroupValues(linkedObj, child, groupSetEntry, groupKeys);
                    }
                }
            }
			if(!hasLinkedEntities && !child.hasUnderlyingQuery())
				nullifyGroupKeys(child, groupSetEntry, groupKeys);
        }
    }
	
	private void nullifyGroupKeys(PathEntry entry, GroupSetEntry groupSetEntry, Set<String>[] groupKeys)
	{
		for (PathEntry field : entry.leafBranches) {
			int groupIndex = field.groupIndex;
			groupSetEntry.m_groupPaths[groupIndex].addValueKeys(groupKeys[groupIndex], null);
		}
		for (PathEntry child : entry.linkBranches) {
			nullifyGroupKeys(child, groupSetEntry, groupKeys);
		}
	}

	// add value to the aggregation groups
	private void updateMetric(String value, GroupSetEntry groupSetEntry, Set<String>[] groupKeys){
		updateMetric(value, groupSetEntry.m_totalGroup, groupKeys, 0);
		if (groupSetEntry.m_isComposite){
			updateMetric(value, groupSetEntry.m_compositeGroup, groupKeys, groupKeys.length - 1);
		}
	}

	// add value to the aggregation group and all subgroups in accordance with the groupKeys paths.
	private synchronized void updateMetric(String value, Group group, Set<String>[] groupKeys, int index){
		group.update(value);
		if (index < groupKeys.length){
			for (String key : groupKeys[index]){
				Group subgroup = group.subgroup(key);
				updateMetric(value, subgroup, groupKeys, index + 1);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void addEmptyGroups(Group group, GroupPath[] groupPathes, int groupPathIndex, boolean useNullGroup) {
		if (group.m_subgroups == null)
			return;
		for (Group subgroup : group.m_subgroups.values()) {
			addEmptyGroups(subgroup, groupPathes, groupPathIndex + 1, useNullGroup);
		}
		GroupPath groupPath = groupPathes[groupPathIndex];
		if (groupPath.converters != null) {
			for (ValueConverter converter : groupPath.converters) {
				if (converter instanceof BatchConverter) {
					BatchConverter batchConverter = (BatchConverter)converter;
					for (String backet : batchConverter.m_backets) {
						if (!group.m_subgroups.containsKey(backet)) {
							group.m_subgroups.put(backet, useNullGroup ? new NullGroup(backet) : new CountGroup(backet));
						}
					}
				}
			}
		}
	}

    private void defineGroupsSelection(Group group, GroupPath[] groupPaths, int index, List<Group> definedgroups) {
        List<String> groupkeys = new ArrayList<String>();
        for (Group subgroup : group.subgroups(groupPaths[index].groupOutputParameters)) {
            groupkeys.add(subgroup.m_key);
        }
        group.defineGroupsSelection(groupkeys); // selection on the first metric function may be defined as well
        if (definedgroups != null) {
            for (Group definedgroup : definedgroups) {
                if (definedgroup != null)
                    definedgroup.defineGroupsSelection(groupkeys);
            }
        }
        index += 1;
        if (index < groupPaths.length) {
            for (Group subgroup : group.m_subgroups.values()) {
                List<Group> definedsubgroups = new ArrayList<Group>();
                for (Group definedgroup : definedgroups) {
                    if (definedgroup != null)
                        definedsubgroups.add(definedgroup.m_subgroups.get(subgroup.m_key));
                }
                defineGroupsSelection(subgroup, groupPaths, index, definedsubgroups);
            }
        }
    }

    private void parseParam_m(String paramValue) {
		m_mParamValue = paramValue;
		List<MetricExpression> me = AggregationQueryBuilder.BuildAggregationMetricsExpression(m_mParamValue, m_tableDef);
		for(MetricExpression m : me) {
			Utils.require(m instanceof AggregationMetric, "Metric expressions are not supported in Spider service");
		}
		List<AggregationMetric> metrics = AggregationQueryBuilder.BuildAggregationMetrics(m_mParamValue, m_tableDef);
		m_metricPaths = new MetricPath[metrics.size()];
		for (int i = 0; i < metrics.size(); i++) {
			AggregationMetric metric = metrics.get(i);
			if (metric.items == null){
				metric.items = new ArrayList<AggregationGroupItem>(1);
				AggregationGroupItem item = new AggregationGroupItem();
				item.tableDef = m_tableDef;
				item.name = PathEntry.ANY;
				item.isLink = false;
				metric.items.add(item);
			}
			m_metricPaths[i] = new MetricPath(metric);
		}
	}

	private void parseParam_q(String paramValue) {
		m_query = paramValue;
	}

	private void parseParam_f(String paramValue) {
		m_fParamValue = paramValue;
		m_groups = AggregationGroup.GetAggregationList(m_fParamValue,  m_tableDef);
	}

	private void parseParam_cf(String paramValue) {
		parseParam_f(paramValue);
		m_isComposite = true;
	}

	private void parseParam_l2r(String paramValue) {
		m_l2rEnable = "true".equals(paramValue);
	}

    private void parseParameters(String[] queryParts) throws IllegalArgumentException {
        HashSet<String> parsedParams = new HashSet<String>();
        for (String queryPart : queryParts) {
            // All parts should have the format "x=y" where x is query parameter
            // and y is the value for the corresponding query part.
            int eqInx = queryPart.indexOf('=');
            Utils.require(eqInx > 0, "Query parameter missing '=': " + queryPart);
            String paramCode = queryPart.substring(0, eqInx);
            String paramValue = queryPart.substring(eqInx + 1);
            if (!parsedParams.add(paramCode)){
                abortParsing("Query parameter can only be specified once: " + queryPart);
            }
            switch (paramCode) {
            case "cf":
                parseParam_cf(paramValue);
                break;
            case "f":
                parseParam_f(paramValue);
                m_groups = AggregationGroup.GetAggregationList(m_fParamValue,  m_tableDef);
                break;
            case "l2r":
                parseParam_l2r(paramValue);
                break;
            case "m":
                parseParam_m(paramValue);
                break;
            case "q":
                parseParam_q(paramValue);
                break;
            default:
                abortParsing("Illegal query parameter: " + queryPart);
            }
        }
    }
    
	private void buildContext() throws IllegalArgumentException {
		// m_groups (as well as m_fParamValue) may be not initialized, m_metrics.size() and m_metricPaths.length are always greater than 0...
		m_showGroupSets = m_metricPaths.length > 1 ||
				(m_groups != null && m_groups.size() > 1) ||
				(m_fParamValue != null && Pattern.matches("GROUP\\(.+\\)", m_fParamValue.toUpperCase()));
		int cGroups = 1;
		if (m_groups != null)
			cGroups = m_groups.size();
		m_groupSet = new GroupSetEntry[ m_metricPaths.length * cGroups];
		for (int im = 0; im <  m_metricPaths.length; im++) {
			for (int ig = 0; ig < cGroups; ig++) {
				int i = im * cGroups + ig;
			    m_groupSet[i] = new GroupSetEntry();
			    if (m_groups == null) {
		            m_groupSet[i].m_groupPaths = new GroupPath[0];
			    } else {	
				    List<AggregationGroup> aggregationGroups = m_groups.get(ig);
		    		StringBuilder groupPathsParam = new StringBuilder();
		    		m_groupSet[i].m_groupPaths = new GroupPath[aggregationGroups.size()];
		    		if (aggregationGroups.size() > 0) {
			    		for (int j = 0; j < aggregationGroups.size(); j++) {
			    			groupPathsParam = groupPathsParam.append(aggregationGroups.get(j).text).append(',');
			    			m_groupSet[i].m_groupPaths[j] = GroupPath.createGroupPath(m_tableDef, aggregationGroups.get(j), j);
			    		}
			    		m_groupSet[i].m_groupPathsParam = groupPathsParam.deleteCharAt(groupPathsParam.length() - 1).toString();
		    		}
			    }
			    MetricPath metricPath = m_metricPaths[im];
    			m_groupSet[i].m_metricPath = new MetricPath(metricPath.metric);
    			m_groupSet[i].m_totalGroup = Group.getGroup(metricPath.function, metricPath.fieldType);			    
			}
		}
		if (m_isComposite) {
			for (GroupSetEntry groupSetEntry : m_groupSet) {
		        if (groupSetEntry.m_groupPaths.length >= 2) {
		            groupSetEntry.m_isComposite = true; // mark each entry in group set individually
		        }
			}
		}
	}
	
	public void parseParameters(UNode rootNode) {
        Utils.require(rootNode.getName().equals("aggregate-search"), "Root node must be 'aggregate-search': " + rootNode);
        StringBuilder uriQuery = new StringBuilder();
        for (String childName : rootNode.getMemberNames()) {
            UNode childNode = rootNode.getMember(childName);
            Utils.require(childNode.isValue(), "Elements of 'aggregate-search' must be values: " + childNode);
            if (uriQuery.length() > 0) {
                uriQuery.append('&');
            }
            if (childName.equals("query")) {
                uriQuery.append("q=");
            } else if (childName.equals("metric")) {
                uriQuery.append("m=");
            } else if (childName.equals("grouping-fields")) {
                uriQuery.append("f=");
            } else if (childName.equals("composite-fields")) {
                uriQuery.append("cf=");
            } else if (childName.equals("pair")) {
                uriQuery.append("pair=");
            } else {
                Utils.require(false, "Unknown 'aggregate-search' parameter: " + childName);
            }
            uriQuery.append(Utils.urlEncode(childNode.getValue()));
        }
        parseParameters(uriQuery.toString());
	}
	
	public void parseParameters(String uriQuery) {
		parseParameters(Utils.splitURIQuery(uriQuery));
		Utils.require(m_metricPaths != null && m_metricPaths.length != 0, "Metric ('m') parameter is required");
		buildContext();
	}

	public void parseParameters(String metricParam, String queryParam, String groupParam) {
		Utils.require(metricParam != null && !metricParam.isEmpty(), "Metric parameter is required");
		// metricParam cannot be empty
    	parseParam_m(metricParam);
		if (queryParam != null && !queryParam.isEmpty())
			parseParam_q(queryParam);
		if (groupParam != null && !groupParam.isEmpty())
			parseParam_f(groupParam);
		buildContext();
	}

	static void abortParsing(String message){
		throw new IllegalArgumentException(message);
	}

    public String[] getGroupNames()
    {
        if(m_fParamValue == null || m_fParamValue.isEmpty())
            return null;
        GroupSetEntry groupSetEntry = m_groupSet[0];
        String[] groupName = new String[groupSetEntry.m_groupPaths.length];
        for(int i = 0; i < groupName.length; i++)
        {
            groupName[i] = groupSetEntry.m_groupPaths[i].name;
        }
        return groupName;
    }

    public boolean isAverageMetric()
    {
        GroupSetEntry groupSetEntry = m_groupSet[0];
        return groupSetEntry.m_totalGroup instanceof AverageGroup;
    }
    
    public StatisticResult getStatisticResult(String statisticName) throws IOException
    {
        StatisticResult result = null;
        GroupSetEntry groupSetEntry = m_groupSet[0];
        if(groupSetEntry.m_totalGroup instanceof AverageGroup)
        {
            result = new AverageStatistic();
            String key = m_tableDef.getTableName() + StatisticResult.KEYSEPARATOR + statisticName;
            result.setFieldName(key + StatisticResult.AVERAGESEPARATOR + StatisticResult.COUNTKEY);
            ((AverageStatistic)result).setSumFieldName(key + StatisticResult.AVERAGESEPARATOR + StatisticResult.SUMKEY);
        }
        else
        {
            result = new StatisticResult();
            result.setFieldName(m_tableDef.getTableName() + StatisticResult.KEYSEPARATOR + statisticName);
        }

        if(groupSetEntry.m_totalGroup.m_subgroups == null)
        {
            putValue(result, StatisticResult.VALUEKEY, groupSetEntry.m_totalGroup);
        }
        if (groupSetEntry.m_totalGroup.m_subgroups != null)
        {
            for (Group group: groupSetEntry.m_totalGroup.subgroups(groupSetEntry.m_groupPaths[0].groupOutputParameters))
            {
                getGroupResult(result, group, groupSetEntry, 0, group.getDisplayName());
            }
            putValue(result, StatisticResult.SUMMARY, groupSetEntry.m_totalGroup);
        }
        if (groupSetEntry.m_isComposite && groupSetEntry.m_compositeGroup.m_subgroups != null)
        {
            getCompositeGroupResult(result, groupSetEntry.m_compositeGroup, groupSetEntry, groupSetEntry.m_groupPaths.length - 1, groupSetEntry.m_groupPaths[0].path);
        }
        return result;
    }

    private void putValue(StatisticResult result, String columnName, Group metricValue)
    {
        if(result instanceof AverageStatistic)
        {
            AverageGroup avarrage = (AverageGroup)metricValue;
            result.addValue(new AbstractMap.SimpleEntry<String, Object>(columnName, avarrage.m_count));
            ((AverageStatistic)result).addSumValue(new AbstractMap.SimpleEntry<String, Object>(columnName, avarrage.getValue()));
        }
        else
        {
            result.addValue(new AbstractMap.SimpleEntry<String, Object>(columnName, metricValue.getMetric()));
        }
    }

    private void getGroupResult(StatisticResult result, Group group, GroupSetEntry groupSetEntry, int groupPathIndex, String parentName) throws IOException
    {
        if (group.m_subgroups != null)
        {
            for (Group subgroup: group.subgroups(groupSetEntry.m_groupPaths[groupPathIndex+1].groupOutputParameters))
            {
                String groupName = parentName + GROUPSEPARATOR + subgroup.getDisplayName();
                getGroupResult(result, subgroup, groupSetEntry, groupPathIndex+1, groupName);
                putValue(result, parentName + GROUPSEPARATOR + StatisticResult.SUMMARY, group);
            }
        }
        else
        {
            putValue(result, parentName, group);
        }
    }

    private void getCompositeGroupResult(StatisticResult result, Group group, GroupSetEntry groupSetEntry, int groupPathIndex, String parentName) throws IOException
    {
        if (group.m_subgroups != null)
        {
            for (Group subgroup: group.subgroups(groupSetEntry.m_groupPaths[groupPathIndex].groupOutputParameters))
            {
                getGroupResult(result, subgroup, groupSetEntry, groupPathIndex, parentName);
            }
        }
    }
    
    public AggregateResult getResult() {
        AggregateResult aggResult = new AggregateResult();
        aggResult.setMetricParam(m_mParamValue);
        if (m_query!= null) {
            aggResult.setQueryParam(m_query);
        }
        if (m_fParamValue != null) {
            aggResult.setGroupingParam(m_fParamValue);
        }
        
        if (!m_showGroupSets && m_fParamValue == null) { // global aggregate
            aggResult.setGlobalValue(m_groupSet[0].m_totalGroup.getMetricValue());
        } else {
            if (!m_showGroupSets) { // single-tree aggregate (compound = false)
                addGroupSet(aggResult, m_groupSet[0], false);
                aggResult.setGlobalValue(m_groupSet[0].m_totalGroup.getMetricValue());
            } else { // multi-tree aggregate (compound = true)
                for (GroupSetEntry groupSetEntry : m_groupSet) {
                    addGroupSet(aggResult, groupSetEntry, true);
                }
            }
        }
        if (m_totalObjects != -1)
            aggResult.setTotalObjects(m_totalObjects);
        
        return aggResult;
    }   // toDoc
    
    public UNode toDoc() {
        return getResult().toDoc();
    }   // toDoc
    
    private void addGroupSet(AggregateResult aggResult, GroupSetEntry groupSetEntry, boolean compound) {
        AggregateResult.AggGroupSet aggGroupSet = aggResult.addGroupSet();
        if (compound) { // set grouping-param, metric-param, or groupset-value for compound aggregates only
            if (m_metricPaths.length > 1) // set metric-param for multi-metrics aggregates only
                aggGroupSet.setMetricParam(groupSetEntry.m_metricPath.metric.sourceText);
            if (groupSetEntry.m_groupPathsParam != null) {
                aggGroupSet.setGroupingParam(groupSetEntry.m_groupPathsParam);
            }
            aggGroupSet.setGroupsetValue(groupSetEntry.m_totalGroup.getMetricValue()); // "value" or "summary"
        }
        
        if (groupSetEntry.m_totalGroup.m_subgroups != null) {
            for (Group group : groupSetEntry.m_totalGroup.subgroups(groupSetEntry.m_groupPaths[0].groupOutputParameters)) {
                addGroup(aggGroupSet, group, groupSetEntry, 0);
            }
            if (groupSetEntry.m_isComposite && groupSetEntry.m_compositeGroup.m_subgroups != null) {
                addCompositeGroup(aggGroupSet, groupSetEntry.m_compositeGroup, groupSetEntry, groupSetEntry.m_groupPaths[0].path, groupSetEntry.m_groupPaths.length - 1);
            }
        }
        if (groupSetEntry.m_totalGroup.m_subgroups != null && groupSetEntry.m_groupPaths[0].groupOutputParameters.count != 0) {
            aggGroupSet.setTotalGroups(groupSetEntry.m_totalGroup.m_subgroups.size());
        }
    }
    	
    private void addGroup(IAggGroupList aggGroupList, Group group, GroupSetEntry groupSetEntry, int groupPathIndex) {
        AggregateResult.AggGroup aggGroup = aggGroupList.addGroup();
        aggGroup.setFieldName(groupSetEntry.m_groupPaths[groupPathIndex].path);
        aggGroup.setFieldValue(group.getDisplayName());
        
        aggGroup.setGroupValue(group.getMetricValue()); // "metric" or "summary"
        
        if (group.m_subgroups != null) {
            for (Group subgroup: group.subgroups(groupSetEntry.m_groupPaths[groupPathIndex + 1].groupOutputParameters)) {
                addGroup(aggGroup, subgroup, groupSetEntry, groupPathIndex+1);
            }
            if (groupSetEntry.m_groupPaths[groupPathIndex + 1].groupOutputParameters.count != 0) {
                aggGroup.setTotalGroups(new Long(group.m_subgroups.size()));
            }
        }
    }
    
    private void addCompositeGroup(AggGroupSet aggGroupSet, Group group, GroupSetEntry groupSetEntry, String name, int groupPathIndex){
        AggregateResult.AggGroup aggGroup = aggGroupSet.addGroup();
        aggGroup.setFieldName(name);
        aggGroup.setFieldValue(group.getDisplayName());
        aggGroup.setComposite(true);

        if (group.m_subgroups != null) {
            for (Group subgroup: group.subgroups(groupSetEntry.m_groupPaths[groupPathIndex].groupOutputParameters)) {
                addGroup(aggGroup, subgroup, groupSetEntry, groupPathIndex);
            }
        }
    }
    
}

class GroupSetEntry {
    String m_groupPathsParam;
    MetricPath m_metricPath;
    GroupPath[] m_groupPaths;

    Group m_totalGroup;
    boolean m_isComposite;
    Group m_compositeGroup;

    static int GetGroupPathsCount(GroupSetEntry[] groupSetEntries) {
        int groupPathsCount = 0;
        for (GroupSetEntry groupSetEntry : groupSetEntries) {
            groupPathsCount += groupSetEntry.m_groupPaths.length;
        }
        return groupPathsCount;
    }
}

class GroupOutputParameters{
	int count;
	Selection function;
	public GroupOutputParameters(Selection function, int count) {
		this.function = function;
		this.count = count;
	}

	public Collection<Group> sortGroups(Collection<Group> collection){
		List<Group> list = new ArrayList<Group>(collection);

		Comparator<Group> comparator;
		if(function == Selection.Top){
			comparator = new Comparator<Group> (){
				@Override
				public int compare(Group group1, Group group2) {
					return -Group.compare(group1, group2);
				}
			};
		}
		else if(function == Selection.Bottom){
			comparator = new Comparator<Group> (){
				@Override
				public int compare(Group group1, Group group2) {
					return Group.compare(group1, group2);
				}
			};
		}
		else {
			comparator = new Comparator<Group> (){
				@Override
				public int compare(Group group1, Group group2) {
					return group1.compareName(group2);
				}
			};
		}
		Collections.sort(list, comparator);
		if (count != 0){
			list = list.subList(0, Math.min(count, list.size()));
		}
		return list;
	}
}

class MetricPath extends PathEntry {
	public MetricPath(AggregationMetric metric) {
		super(metric.items, 0, 0, false);
		this.metric = metric;
		this.function = metric.function;
		FieldDefinition fieldDef = metric.items.get(metric.items.size()-1).fieldDef;
		if (fieldDef != null) {
			this.fieldType = fieldDef.getType();
		}
	}
	FieldType fieldType;
	String function;
	AggregationMetric metric;
}

class GroupPath extends PathEntry {
    List<ValueConverter> converters;
    ValueExcludeInclude excludeinclude;
    ValueTokenizer tokenizer;
	GroupOutputParameters groupOutputParameters;
	String path;
	AggregationGroup aggregationGroup;

	public GroupPath(AggregationGroup group, int groupIndex) {
		super(group.items, 0, groupIndex, true);
		path = group.name != null ? group.name : toString(group.items); // use alias (if defined) instead of group path
	}

	static GroupPath createGroupPath(TableDefinition tableDef, AggregationGroup path, int index){

		GroupOutputParameters outputParams = new GroupOutputParameters(path.selection, path.selectionValue);

		GroupPath entry = new GroupPath(path, index);
		entry.groupOutputParameters = new GroupOutputParameters(path.selection, path.selectionValue);
		if (path.tocase != null || path.truncate != null || path.batch != null) {
			entry.converters = new ArrayList<ValueConverter>();
			if (path.tocase != null){
				entry.converters.add(CaseConverter.getConverter(path.tocase));
			}
			if (path.truncate != null){
				if (path.timeZone != null){
					entry.converters.add(new TimeZoneConverter(path.timeZone));
				}
				entry.converters.add(DateConverter.getConverter(path.truncate));
			}
			if (path.batch != null){
				entry.converters.add(BatchConverter.getConverter(path.batch));
			}
		}

		if (path.exclude != null || path.include != null) {
			entry.excludeinclude = new ValueExcludeInclude(path.exclude, path.include);
		}

		if (path.stopWords != null) {
		    entry.tokenizer = new TextTokenizer(path.stopWords);
		}

		entry.groupOutputParameters = outputParams;
		entry.aggregationGroup = path;
		return entry;
	}

	void addValueKeys(Set<String> keys, String value) {
        if (excludeinclude != null) {
        	if (!excludeinclude.accept(value))
        		return;
        }
        if (value != null) {
	        if (converters != null) {
	            for (ValueConverter converter : converters) {
	                value = converter.convert(value);
	            }
	        }
	        if (tokenizer != null) {
	            Collection<String> tokens = tokenizer.tokenize(value);
	            if (tokens != null && tokens.size() > 0) {
	                keys.addAll(tokens);
	            }
	            return;
	        }
        }
        keys.add(value == null ? Group.NULL_GROUP_NAME : value);
	}
}

class PathEntry {
	
    static final boolean USEQUERYCACHE = true;
    static final int QUERYCACHECAPACITY = 10000;

	static final String ANY = "*";

	TableDefinition tableDef;
	//String path;
	String name;
	Query query;
	String queryText;
	LRUCache<ObjectID, Boolean> queryCache;
    List<LinkInfo> nestedLinks;
	Filter filter;
	boolean isLink;
	int groupIndex = 0;
	List<Integer> groupIndexes = new ArrayList<Integer>();
	List<Integer> matchingGroupIndexes = new ArrayList<Integer>();
	List<String> fieldNames = new ArrayList<String>();
	List<PathEntry> leafBranches = new ArrayList<PathEntry>();
	List<PathEntry> linkBranches = new ArrayList<PathEntry>();
	List<PathEntry> branches = new ArrayList<PathEntry>();

	public PathEntry(TableDefinition tableDef, int groupIndex) {
		this.tableDef = tableDef;
		this.groupIndex = groupIndex;
	}

	public PathEntry(List<AggregationGroupItem> path, int index, int groupIndex, boolean isGroupPath) {
		AggregationGroupItem item = path.get(index);
		this.tableDef = item.tableDef;
		this.groupIndex = groupIndex;
		name = item.name;
        nestedLinks = item.nestedLinks;
		isLink = item.isLink;
		PathEntry entry = null;
		if(index == path.size() - 1) {
			if(isLink) {
				entry = new PathEntry(item.tableDef, groupIndex);
				entry.name = ANY;
				entry.isLink = false;
				add(entry, isGroupPath);
			}
			else if(name != PathEntry.ANY) {
				fieldNames.add(name);
			}
		}
		else {
			entry = new PathEntry(path, index + 1, groupIndex, isGroupPath);
			add(entry, isGroupPath);
		}
		if (item.query != null) {
			setQuery(item, isGroupPath ? this : entry);
		}
	}

	void add(PathEntry entry, boolean isGroupPath) {
		branches.add(entry);
		if (entry.isLink){
			if (isGroupPath){
				linkBranches.add(entry);
			}
		}
		else {
			if (isGroupPath){
				leafBranches.add(entry);
			}
			if (!entry.name.startsWith(ANY)){
				if (!fieldNames.contains(entry.name)){
					fieldNames.add(entry.name);
				}
			}
		}
	}
	
	static void setQuery(AggregationGroupItem item, PathEntry entry) {
		entry.query = item.query;
		if (entry.query != null) {
			entry.queryText = item.query.toString();
			if (USEQUERYCACHE) {
				entry.queryCache = new LRUCache<ObjectID, Boolean>(QUERYCACHECAPACITY);
			}
			QueryExecutor qe = new QueryExecutor(item.tableDef);
			entry.filter = qe.filter(item.query);
		}
	}
	

	void addGroupPath(PathEntry anotherPath) {
		//PathEntry next = branches.get(0);
		if(name.equals(anotherPath.name)){
			mergeParameters(this,anotherPath);
			if (branches.size()==0 || anotherPath.branches.size()==0){
				matchingGroupIndexes.add(anotherPath.groupIndex);
//				merge(1, anotherPath);
//				Aggregate.abortParsing(String.format("Metric and group paths can't coincide"));
			}
			else {
				branches.get(0).addGroupPath(anotherPath.branches.get(0));
			}
		}
		else {
			groupIndexes.add(anotherPath.groupIndex);
			mergeBranch(anotherPath);
		}
	}

	void mergeBranch(PathEntry entry) {
		String name = entry.name;
		PathEntry branch = null;
		for (int i=0; i < branches.size(); i++){
			if (branches.get(i).name.equals(name)){
				branch = branches.get(i);
				mergeParameters(branch, entry);
				break;
			}
		}
		if (branch == null){
			add(entry, true);
		}
		else if (entry.branches.size() > 0){
			branch.mergeBranch(entry.branches.get(0));
		}
	}

    boolean checkCondition(Entity entity) {
        if (USEQUERYCACHE && queryCache.containsKey(entity.id())) return queryCache.get(entity.id());
        boolean result = filter.check(entity);
        if (USEQUERYCACHE) queryCache.put(entity.id(), result);
        return result;
    }

	void mergeParameters(PathEntry entry1, PathEntry entry2){
		for (String name:entry2.fieldNames){
			if (!entry1.fieldNames.contains(name)){
				entry1.fieldNames.add(name);
			}
		}
		if (entry2.query!=null){
			if (entry1.query != null){
				 AndQuery query = new AndQuery();
				 query.subqueries.add(entry1.query);
				 query.subqueries.add(entry2.query);
				 entry1.query = query;
			}
			else {
				entry1.query = entry2.query;
				if (USEQUERYCACHE) {
					entry1.queryCache = new LRUCache<ObjectID, Boolean>(QUERYCACHECAPACITY);
				}
			}
		    QueryExecutor qe = new QueryExecutor(entry1.tableDef);
		    entry1.filter = qe.filter(entry1.query);
		}
	}

	boolean hasUnderlyingQuery() {
		if (query != null)
			return true;
		for (PathEntry child : linkBranches) {
			if (child.hasUnderlyingQuery()) {
				return true;
			}
		}
		return false;
	}

	String toString(String indent){
		String text = indent + name;
		for (PathEntry entry: branches){
			text += "\n" + entry.toString(indent + "  ");
		}
		return text;
	}

	@Override
	public String toString(){
		return toString("");
	}

	public static String toString(List<AggregationGroupItem> path){
		if (path==null|| path.size()==0){
			return "";
		}
		StringBuilder builder = new StringBuilder();
		builder.append(path.get(0).name);
		for (int i = 1; i < path.size(); i++){
			builder.append('.').append(path.get(i).name);
		}
		return builder.toString();
	}

}

