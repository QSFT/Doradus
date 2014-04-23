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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.search.aggregate.Aggregate.StatisticResult;
import com.dell.doradus.service.statistic.TaskInfo.Status;

public class StatisticRunner implements Runnable {

    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
	private final StatisticDefinition m_statDef;
	private final String m_appName;
	private final TaskInfo m_taskInfo = new TaskInfo();
	
	private StatisticResult m_statistic;
	
    public StatisticRunner(String appName, StatisticDefinition statDef) {
    	m_appName = appName;
    	m_statDef = statDef;
    }
    
    public String getAppName() { return m_appName; }
    
    public StatisticDefinition getStatDefinition() { return m_statDef; }
    
    public TaskInfo getTaskInfo() { return m_taskInfo; }
    
    public StatisticResult getStatistic() { return m_statistic; }
    
	public void setResult(StatisticResult statistic) {
    	m_statistic = statistic;
	}
	
    public void run() {
    	StatisticManager manager = StatisticManager.instance();
		m_logger.debug("Recalculation started. Statistic: " + StatisticManager.getTaskKey(m_appName, m_statDef));
		m_taskInfo.taskStatus = Status.STARTED;
		m_taskInfo.timeStarted = System.currentTimeMillis();
		
		// Start a thread that will stamp a refreshing task as live until finished.
		Thread stampingThread = new Thread(new Runnable() {
			public void run() {
				try {
					while (true) {
						Thread.sleep(2 * ServerConfig.getInstance().task_exec_delay * 1000);
						StatStatusStorageManager.stamp(
								m_appName, m_statDef.getTableName(), m_statDef.getStatName());
					}
				} catch (InterruptedException e) {
					// Just stop
				}
			}
		}, "Stamper");
		stampingThread.start();
		
		manager.updateTaskInfo(
				m_appName, m_statDef.getTableName(), m_statDef.getStatName(), m_taskInfo);
		try 
		{
			Mapper.map(this);
			m_taskInfo.taskStatus = Status.MAPPED;
			m_taskInfo.timeMapped = System.currentTimeMillis();
			manager.updateTaskInfo(
					m_appName, m_statDef.getTableName(), m_statDef.getStatName(), m_taskInfo);
			m_logger.debug("Statistic mapped." 
					+ " time: " + getElapsedTime(m_taskInfo.timeStarted, m_taskInfo.timeMapped)
					+ ". Statistic: " + StatisticManager.getTaskKey(m_appName, m_statDef));
		} 
		catch (Throwable e) 
		{
			m_taskInfo.taskStatus = Status.FAILED;
			m_taskInfo.timeFinished = System.currentTimeMillis();
			m_taskInfo.errorMessage = e.getMessage();
			manager.updateTaskInfo(
					m_appName, m_statDef.getTableName(), m_statDef.getStatName(), m_taskInfo);
			m_logger.error("Mapper failed."
					+ " time: " + getElapsedTime(m_taskInfo.timeStarted, m_taskInfo.timeFinished)
					+ ". Statistic: " + StatisticManager.getTaskKey(m_appName, m_statDef));
			m_logger.error(e.getMessage());
			// stop stamping thread
			stampingThread.interrupt();
			return;
		} 
        try 
        {
			Reducer.reduce(this);
			m_taskInfo.taskStatus = Status.REDUCED;
			m_taskInfo.timeFinished = System.currentTimeMillis();
			manager.updateTaskInfo(
					m_appName, m_statDef.getTableName(), m_statDef.getStatName(), m_taskInfo);
			m_logger.debug("Statistic reduced." 
					+ " time: " + getElapsedTime(m_taskInfo.timeStarted, m_taskInfo.timeFinished)
					+ ". Statistic: " + StatisticManager.getTaskKey(m_appName, m_statDef));
		} 
        catch (Throwable e) 
        {
        	m_taskInfo.taskStatus = Status.FAILED;
        	m_taskInfo.timeFinished = System.currentTimeMillis();
			m_taskInfo.errorMessage = e.getMessage();
			manager.updateTaskInfo(
					m_appName, m_statDef.getTableName(), m_statDef.getStatName(), m_taskInfo);
			m_logger.error("Reducer failed."
					+ " time: " + getElapsedTime(m_taskInfo.timeStarted, m_taskInfo.timeFinished)
					+ ". Statistic: " + StatisticManager.getTaskKey(m_appName, m_statDef));
			m_logger.error(e.getMessage());
			return;
		}
        finally {
        	// stop stamping thread
			stampingThread.interrupt();
        }
    }

	private static String getElapsedTime(long timeStart, long timeEnd)
	{
		return Utils.formatElapsedTime(timeEnd - timeStart);
	}

}
