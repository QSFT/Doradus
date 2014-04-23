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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.StatResult;
import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.StatsStatus;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.search.aggregate.Aggregate;
import com.dell.doradus.service.statistic.TaskInfo.Status;
import com.dell.doradus.service.taskmanager.TaskManagerService;
import com.dell.doradus.tasks.DoradusTask;

/**
 * Implementation of a service that performs queries for statistics updating
 * and retrieving.
 */
public final class StatisticManager {
	// Singleton
	private static StatisticManager INSTANCE = new StatisticManager();
	
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
	// StatisticManager instance is accessible via singleton only.
	private StatisticManager() {}
	
	// Access to the singleton
	public static StatisticManager instance() { return INSTANCE; }
	
	/**
	 * Get the recalculation status of all statistics owned by the given table and
	 * return a result as a {@link StatsStatus} object.
	 * 
	 * @param tabDef	{@link TableDefinition} table to get statistics from it
	 * @return			Statistics recalculation status
	 */
	public StatsStatus getStatStatus(TableDefinition tabDef) {
	    // Create a stats-status response object.
	    StatsStatus result = new StatsStatus();
	    
	    String appName = tabDef.getAppDef().getAppName();
		for (StatisticDefinition statDef : tabDef.getStatDefinitions()) {
            //TaskInfo taskInfo = m_taskInfoMap.get(getTaskKey(appName, statDef));
            TaskInfo taskInfo = StatStatusStorageManager.get(appName, statDef.getTableName(), statDef.getStatName());
            if (taskInfo == null) {
                result.addStatus(statDef, "Statistic has not been scheduled for recalculation");
            } else {
                result.addStatus(statDef, taskInfo.getStatus());
            }
		}
		return result;
	}	// getStatStatus
	
	public StatResult getStatistics(ApplicationDefinition appDef, StatisticDefinition statDef, String params) {
        TableDefinition tableDef = appDef.getTableDef(statDef.getTableName());
        Aggregate aggregate = new Aggregate(tableDef);
        aggregate.parseParameters(statDef.getMetricParam(), statDef.getQueryParam(), statDef.getGroupParam());
        // Create a stat query with the basic parameters.
        StatisticQuery statQuery = new StatisticQuery(appDef.getAppName(),
                                                      statDef,
                                                      aggregate.getGroupNames(),
                                                      aggregate.isAverageMetric());
        statQuery.processParams(params);
        
        try {
            // Execute query.
        	return statQuery.execute();
        } catch (IOException e) {
        	m_logger.error("Statistics query failed with " + e.getMessage());
        	throw new IllegalArgumentException("Statistic query failed");
        }
	}	// getStatistics
	
	public boolean initTask(String appName, StatisticDefinition statDef) {
		TaskInfo taskInfo = StatStatusStorageManager.get(appName, statDef.getTableName(), statDef.getStatName());
		if (taskInfo != null && taskInfo.isRunning() && taskInfo.isFresh()) {
			return false;
		}
		
		taskInfo = new TaskInfo(Status.DEFINED);
		taskInfo.timeDeclared = System.currentTimeMillis();

		StatStatusStorageManager.put(appName, statDef.getTableName(), statDef.getStatName(), taskInfo);

		return true;
	}	// initTask
	
	public boolean initTask(String appName, TableDefinition tabDef) {
		for (StatisticDefinition statDef : tabDef.getStatDefinitions()) {
			TaskInfo taskInfo = StatStatusStorageManager.get(appName, statDef.getTableName(), statDef.getStatName());
			if (taskInfo != null && taskInfo.isRunning() && taskInfo.isFresh()) {
				return false;
			}
		}
		
		for (StatisticDefinition statDef : tabDef.getStatDefinitions()) {
			TaskInfo taskInfo = new TaskInfo(Status.DEFINED);
			taskInfo.timeDeclared = System.currentTimeMillis();

			StatStatusStorageManager.put(appName, statDef.getTableName(), statDef.getStatName(), taskInfo);
		}
		
		return true;
	}

	public boolean refreshStatistic(String appName, StatisticDefinition statDef) {
		boolean canStart = initTask(appName, statDef);
		if (canStart) {
			DoradusTask task = DoradusTask.createTask(appName, statDef.getTableName(), SchedType.STAT_REFRESH.getName(), statDef.getStatName());
			if (task != null) {
				TaskManagerService.instance().startTask(task);
			}
		}
		return canStart;
	}	// refreshStatistic
	
	public boolean refreshStatistic(String appName, TableDefinition tabDef) {
		boolean canStart = initTask(appName, tabDef);
		if (canStart) {
			DoradusTask task = DoradusTask.createTask(appName, tabDef.getTableName(), SchedType.STAT_REFRESH.getName(), null);
			if (task != null) {
				TaskManagerService.instance().startTask(task);
			}
		}
		return canStart;
	}	// refreshStatistic
	
	public TaskInfo getTaskInfo(String appName, StatisticDefinition statDef) {
		return StatStatusStorageManager.get(appName, statDef.getTableName(), statDef.getStatName());
	}

	public static String getTaskKey(String appName, StatisticDefinition statDef)
	{
		return appName + "/" + statDef.getTableName() + "/" + statDef.getStatName();
	}	// getTaskKey
	
	public void updateTaskInfo(String appName, String tabName, String statName, TaskInfo taskInfo) {
		StatStatusStorageManager.put(appName, tabName, statName, taskInfo);
	}	// updateTaskInfo
}
