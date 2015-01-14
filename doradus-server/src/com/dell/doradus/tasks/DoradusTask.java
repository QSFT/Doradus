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

package com.dell.doradus.tasks;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.management.TaskRunState;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;
import com.dell.doradus.service.taskmanager.TaskDBUtils;
import com.dell.doradus.service.taskmanager.TaskManagerService;

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;

/**
 * Basic implementation of background task
 */
public abstract class DoradusTask extends Task {
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    // Storage service names
	final static String OLAP_SERVICE_NAME = OLAPService.class.getSimpleName();
	final static String SPIDER_SERVICE_NAME = SpiderService.class.getSimpleName();
	
	// Task context parameters names
	final static String APP_NAME = "APP_NAME";
	final static String TASK_ID = "TASK_ID";
	
	// Parameters
	protected Tenant    m_tenant;
	protected String 	m_taskId;
	protected String 	m_taskType;
	protected ApplicationDefinition m_appDef;
	protected String 	m_appName;
	protected String 	m_tableName;
	protected String 	m_taskParam;
	protected String 	m_serviceName;
	private Map<String, String> m_extraParams = new HashMap<>();
	
	// Number of fails
	protected int 		m_fails = 0;
	
	// Executing task parameters
	protected TaskExecutionContext m_taskContext;
	protected long m_scheduledStartTime;
	
	// Access functions
	public Tenant getTenant() { return m_tenant; }
	
	public String getTaskId() { return m_taskId; }
	
	public String getTaskType() { return m_taskType; }
	
	public String getAppName() { return m_appName; }
	
	public String getTableName() { return m_tableName; }
	
	public String getParameter() { return m_taskParam; }
	
	public String getExtraParam(String param) {
		return m_extraParams.get(param);
	}
	
	public void setExtraParam(String parName, String parValue) { 
		m_extraParams.put(parName, parValue);
	}
	
	public TaskExecutionContext getExecutingContext() { return m_taskContext; }
	
	public long getScheduledTime() { return m_scheduledStartTime; }
	
	public int getFails() {
		return m_fails;
	}
	
	public void addFail() {
		m_fails++;
	}
	
	public String getServiceName() { return m_serviceName; }
	
	public boolean isOlap() { return "OLAPService".equals(m_serviceName); }
	
	public boolean isSpider() { return "SpiderService".equals(m_serviceName); }
	
	public static DoradusTask createTask(Tenant tenant, String appName, String tableName, String taskType, String taskParam) {
		ApplicationDefinition app = SchemaService.instance().getApplication(tenant, appName);
		if (app == null) {
			// Application doesn't exist anymore
			return null;
		}
		// Extract class name for the task
		SchedType schedType = SchedType.getByName(taskType);
		if (schedType == null) {
			return null;
		}
		String taskClass = schedType.getClassName();
		if (taskClass == null) {
			return null;
		}
		
		try {
			DoradusTask task = (DoradusTask)Class.forName(taskClass).newInstance();
			StringBuilder taskId = new StringBuilder();
			if (Utils.isEmpty(tableName)) {
				taskId.append("*/");
			} else {
				taskId.append(tableName + "/");
			}
			taskId.append(taskType);
			if (!Utils.isEmpty(taskParam)) {
				taskId.append("/" + taskParam);
			}
			task.m_tenant = tenant;
			task.m_taskId = taskId.toString();
			task.m_taskType = taskType;
			task.m_appDef = app;
			task.m_appName = appName;
			task.m_tableName = tableName;
			task.m_taskParam = taskParam;
			task.m_serviceName = app.getStorageService();
			if (task.m_serviceName == null) {
				task.m_serviceName = SpiderService.class.getSimpleName();
			}
			return task;
		} catch (ClassCastException | InstantiationException 
				| IllegalAccessException | ClassNotFoundException e) {
			return null;
		}
	}	// createTask
	
	/**
	 * Creates a DoradusTask class instance for executing.
	 * @param tenant    Tenant in which application exists.
	 * @param appName	Task's application name
	 * @param taskId	Task's identifier (table/type/arguments)
	 * @return			task created
	 */
	public static DoradusTask createTask(Tenant tenant, String appName, String taskId) {
		if (taskId == null) {
			return null;
		}
		String[] taskParts = taskId.split("/");
		if (taskParts.length < 2) {
			return null;
		}
		String tableName = taskParts[0];
		String taskType = taskParts[1];
		String taskParam = null;
		if (taskParts.length > 2) {
			taskParam = taskParts[2];
		}
		
		return createTask(tenant, appName, tableName, taskType, taskParam);
	}
	
	@Override
	public void execute(TaskExecutionContext ctx) throws RuntimeException {
		if (TaskManagerService.instance().runningTasksNumber() > ServerConfig.getInstance().max_node_tasks) {
			// Don't start more tasks than I'm capable to run; maybe some other node...
			return;
		}
		long startTime = System.currentTimeMillis() / 60000 * 60000;  // truncated to minutes
		
		if (m_fails > 0 || TaskDBUtils.claim(m_tenant, m_appName, m_taskId, startTime)) {
			// Restart OR this node wins a competition
			m_scheduledStartTime = startTime;
			m_taskContext = ctx;
			TaskDBUtils.startTask(this);
			// Indicate normal finishing
			ctx.setStatusMessage(TaskRunState.Succeeded.name());
			m_logger.info("Task {} started appName = {}, serviceName={}", 
					new String[] { m_taskType, m_appName, m_serviceName });
			runTask();
			m_logger.info("Task {} finished with status <{}>", 
					m_taskType, ctx.getTaskExecutor().getStatusMessage());
		}
	}
	
	@Override
	public boolean supportsStatusTracking() { return true; }
	
	@Override
	public boolean canBeStopped() { return true; }
	
	/**
	 * A function that does real work.
	 */
	abstract protected void runTask();
	
	/**
	 * Implementations of runTask() methods must periodically check this flag
	 * to know whether somebody has claimed for task stop. If the flag is set on,
	 * the task should stop working as soon as possible.
	 * @return
	 */
	protected boolean isInterrupted() {
		if (m_taskContext.isStopped()) {
			m_taskContext.setStatusMessage(TaskRunState.Interrupted.name());
			return true;
		}
		return false;
	}

}
