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
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.StatResult;
import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.StatResult.StatGroup;
import com.dell.doradus.search.aggregate.Aggregate;
import com.dell.doradus.search.aggregate.Statistic;
import com.dell.doradus.search.aggregate.Aggregate.StatisticResult;
import com.dell.doradus.search.aggregate.Statistic.StatisticParameter;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;

public class StatisticQuery 
{
    private static final int MAX_GROUP_LEVELS = 100;
    
    private final ApplicationDefinition	m_appDef;
    private final String				m_appName;
    private final String            	m_tableName;
    private final String           		m_statName;
    private final StatisticDefinition	m_statDefinition;
    private final boolean				m_bAverageMetric;
    private String[]					m_groupNames;
    private StatisticParameter[]		m_groupParams;
    private String                      m_startColName = "";
    private String                      m_endColName = "";
    private boolean                     m_bGrouped;

    /**
     * Constructor without server argument is used in "new" services environment.
     * 
     * @param appName
     * @param statDefinition
     * @param groupNames
     * @param bAverageMetric
     */
    public StatisticQuery(String appName, StatisticDefinition statDefinition, String[] groupNames, boolean bAverageMetric) {
        // Prerequisites:
        assert appName != null && appName.length() > 0;
        assert statDefinition != null;
        
        // Capture parameters
        m_appDef = SchemaService.instance().getApplication(appName);
        m_appName = appName;
        m_statDefinition = statDefinition;
        m_statName = statDefinition.getStatName();
        assert m_statName != null && m_statName.length() > 0;
        m_tableName = statDefinition.getTableName();
        assert m_tableName != null && m_tableName.length() > 0;
        m_groupNames = groupNames;
        m_bAverageMetric = bAverageMetric;
        m_bGrouped = (m_groupNames != null && m_groupNames.length > 0);
    }	// constructor
    
	public StatResult execute() throws IOException {
		DBService dbService = DBService.instance();
		String statsStoreName = SpiderService.statsStoreName(m_appName);
		
        final SortedMap<String, Object> statMap = new TreeMap<String, Object>();
        
        // Do a single-row/column-range fetch on the Counters table using the row key:
        //      <table name>/<field name>
        String rowKey = m_tableName + StatisticResult.KEYSEPARATOR + m_statName;
    	Iterator<DColumn> iColumns = dbService.getColumnSlice(
    	        Tenant.getTenant(m_appDef),
    			statsStoreName, 
    			m_bAverageMetric ? rowKey + StatisticResult.AVERAGESEPARATOR + StatisticResult.SUMKEY : rowKey, 
    			m_startColName, m_endColName);
    	while (iColumns.hasNext()) {
    		DColumn nextCol = iColumns.next();
    		if (!m_bGrouped || !isOutOfRange(m_groupParams, nextCol.getName())) {
    			statMap.put(nextCol.getName(), nextCol.getValue());
    		}
    	}
		
        if(!m_bAverageMetric) {
            if(!statMap.containsKey(StatisticResult.SUMMARY)) {
            	DColumn summary =
            	    dbService.getColumn(Tenant.getTenant(m_appDef), statsStoreName, rowKey, StatisticResult.SUMMARY);
            	if(summary != null && summary.getValue() != null)
            		statMap.put(StatisticResult.SUMMARY, summary.getValue());
            }
        } else {
        	String sumKey = rowKey + StatisticResult.AVERAGESEPARATOR + StatisticResult.SUMKEY;
        	String countKey = rowKey + StatisticResult.AVERAGESEPARATOR + StatisticResult.COUNTKEY;
        	Iterator<DColumn> iSumColumn =
        	    dbService.getColumnSlice(Tenant.getTenant(m_appDef), statsStoreName, countKey, m_startColName, m_endColName);
            while (iSumColumn.hasNext()) {
            	DColumn nextCol = iSumColumn.next();
            	if (statMap.containsKey(nextCol.getName())) {
            		Long counterValue = Long.parseLong(nextCol.getValue());
            		if (counterValue != 0) {
            			Long value = Long.parseLong(statMap.remove(nextCol.getName()).toString());
            			Double dValue = (double)value / (double)counterValue;
            			statMap.put(nextCol.getName(), dValue);
            		}
            	}
            }

            if (!statMap.containsKey(StatisticResult.SUMMARY)) {
            	String sumValue =
            	    dbService.getColumn(Tenant.getTenant(m_appDef), statsStoreName, sumKey, StatisticResult.SUMMARY).getValue();
            	if(sumValue != null) {
            		Long value = Long.parseLong(sumValue);
            		Long counterValue = Long.parseLong(
            				dbService.getColumn(Tenant.getTenant(m_appDef), statsStoreName, countKey, 
            						StatisticResult.SUMMARY).getValue());
            		if(counterValue != 0) {
            			Double dValue = (double)value / (double)counterValue;
            			statMap.put(StatisticResult.SUMMARY, dValue);
            		}
            	}
            }

        }
        return createResult(statMap);
	}
	
	private StatResult createResult(Map<String, Object> statMap) {
        StatResult result = new StatResult(m_statDefinition);
        if (Utils.isEmpty(m_statDefinition.getGroupParam())) {
            if (statMap.size() > 0) {
                assert statMap.size() == 1;
                result.setGlobalValue(statMap.values().iterator().next().toString());
            }
            return result;
        }
        
        // Maintain a stack of GroupStats.
        StatGroup[] groupStatList = new StatGroup[MAX_GROUP_LEVELS];
        String[] prevGroups = null;
        for (Map.Entry<String, Object> mapEntry : statMap.entrySet()) {
            String[] groups = mapEntry.getKey().split(Aggregate.GROUPSEPARATOR);
            int lastGroupIndex = groups.length - 1;
          
            // Create or verify group nodes for group names 0 through N-1.
            for (int i = 0; i < lastGroupIndex; i++) {
                if (prevGroups == null || !prevGroups[i].equalsIgnoreCase(groups[i])) {
                    StatGroup statGroup = null;
                    if (i == 0) {
                        statGroup = result.addStatGroup();
                    } else {
                        statGroup = groupStatList[i].addGroup();
                    }
                    statGroup.setFieldName(m_groupNames[i]);
                    statGroup.setFieldValue(groups[i]);
                    groupStatList[i] = statGroup;
                    prevGroups = null;
                }
            }
            
            // Create leaf node for group name N.
            String value = mapEntry.getValue() == null ? "" :  mapEntry.getValue().toString();
            if (groups[lastGroupIndex].equalsIgnoreCase(StatisticResult.SUMMARY)) {
                if (lastGroupIndex == 0) {
                    result.setGlobalValue(value);
                } else {
                    groupStatList[lastGroupIndex - 1].setGroupValue(value);
                }
            } else {
                StatGroup statGroup = null;
                if (lastGroupIndex == 0)  {
                    statGroup = result.addStatGroup();
                } else {
                    statGroup = groupStatList[lastGroupIndex - 1].addGroup();
                }
                statGroup.setFieldName(m_groupNames[lastGroupIndex]);
                statGroup.setFieldValue(groups[lastGroupIndex]);
                statGroup.setGroupValue(value);
            }
            prevGroups = groups; 
        }
        return result;
    }   // createResult

    public void processParams(String params) 
	{
		m_startColName = "";
		m_endColName = "";
		if(params == null || params.isEmpty())
		{
			return;
		}
		if(m_groupNames == null || m_groupNames.length == 0)
		{
			throw new IllegalArgumentException("Parameters are allowed only for grouped statistics");
		}
		String[] statParams = Utils.splitURIQuery(params);
		if(statParams.length != 0)
		{
			m_groupParams = new StatisticParameter[m_groupNames.length];
		}
		for(int i = 0; i < statParams.length; i++)
		{
			StatisticParameter p = 	Statistic.GetStatisticParameters(statParams[i]);
			if(p.level < 1 || p.level > m_groupParams.length)
			{
				throw new IllegalArgumentException("Group level in parameter is out of range of statistic groups: " + p.level);
			}
			m_groupParams[p.level - 1] = p;
		}
		if(m_groupParams[0] != null)
		{
			m_startColName = m_groupParams[0].minValue;
			m_endColName = m_groupParams[0].maxValue + 0xFFFF;
		}
	}
	private static boolean isOutOfRange(StatisticParameter[] m_groupParams, String name) 
	{
		if(m_groupParams == null)
			return false;
		String[] groupNames = name.split(Aggregate.GROUPSEPARATOR);
		int groupCounts = groupNames.length;
		if(groupNames[groupNames.length - 1].compareToIgnoreCase(StatisticResult.SUMMARY) == 0)
			groupCounts--;
		groupCounts = Math.min(groupCounts, m_groupParams.length);
		for(int i = 1; i < groupCounts; i++)
		{
			if(m_groupParams[i] != null)
			{
				if(groupNames[i].compareTo(m_groupParams[i].minValue) < 0)
					return true;
				if(groupNames[i].compareTo(m_groupParams[i].maxValue +  0xFFFF) > 0)
					return true;
			}
		}
		return false;
	}
}
