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

package com.dell.doradus.service.taskmanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ScheduleDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.core.Defs;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.management.TaskRunState;
import com.dell.doradus.management.TaskStatus;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.OlapTasks;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.StoreTemplate;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.tasks.DoradusTask;

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.TaskTable;

/**
 * A set of utilities to process Tasks table
 */
public class TaskDBUtils {
	
	private static Random s_rnd = new Random();
	
	/**
	 * Reads all the applications and gets schedules from them. This is called every
	 * minute from tasks scheduler to retrieve the information about tasks schedules.
	 * 
	 * @return	tasks schedules in a format that the scheduler understands.
	 */
	public static TaskTable getTasksSchedule() {
		TaskTable taskTable = new TaskTable();
        for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications()) {
        	String appName = appDef.getAppName();
        	String appDefaultSchedule = ServerConfig.getInstance().default_schedule;
        	Map<String, String> tableDefSchedules = new HashMap<String, String>();
        	Map<String, String> taskSchedules = new HashMap<String, String>();
        	for (ScheduleDefinition schedDef : appDef.getSchedules().values()) {
        		SchedType schedType = schedDef.getType();
    			if (SchedType.APP_DEFAULT == schedType) {
    				appDefaultSchedule = schedDef.getSchedSpec();
    			} else if (SchedType.TABLE_DEFAULT == schedType) {
    				tableDefSchedules.put(schedDef.getTableName(), schedDef.getSchedSpec());
    			} else {
    				String taskId = schedDef.getTableName();
    				if (taskId == null) taskId = "*";
    				taskId += '/' + schedType.getName();
    				if (schedDef.getTaskDeclaration() != null) {
    					taskId += '/' + schedDef.getTaskDeclaration();
    				}
    				taskSchedules.put(taskId, schedDef.getSchedSpec());
    			}
        	}
        	
        	// Define undefined schedules as default values
        	for (Map.Entry<String, String> schedule : taskSchedules.entrySet()) {
        		if (schedule.getValue() == null) {
        			String tabName = schedule.getKey().split("/")[0];
        			String defSched = tableDefSchedules.get(tabName);
        			if (defSched == null) {
        				defSched = appDefaultSchedule;
        			}
        			schedule.setValue(defSched);
        		}
        	}
        	
        	// Add to tasks table
        	for (Map.Entry<String, String> schedule : taskSchedules.entrySet()) {
        		
    			String schedValue = schedule.getValue();
    			String taskId = schedule.getKey();
        		if (schedValue != null) {
        			// Check status
        			TaskStatus status = getTaskStatus(appName, taskId);
        			if (status.isSchedulingSuspended() || status.isExecuting()) {
        				continue;
        			}
        			
        			try {
        				SchedulingPattern pattern = new SchedulingPattern(schedValue);
        				DoradusTask task = DoradusTask.createTask(appName, taskId);
            			if (task != null) {
            				taskTable.add(pattern, task);
            			}
        			} catch (InvalidPatternException e) {
        				// Just skip the task
        			}
        		}
        	}
        }
        
		return taskTable;
	}	// getTasksSchedule
	
	/**
	 * Returns a table of all the registered tasks (some tasks may be non-existing
	 * by current time) for all the applications.
	 * 
	 * @return	Map of application names to the set of registered task names.
	 */
	public static Map<String, Set<String>> getAllTaskNames() {
		Set<String> appNames = DBService.instance().getAllAppProperties().keySet();
		Map<String, Set<String>> allTaskNames = new HashMap<>();
		for (String appName : appNames) {
			allTaskNames.put(appName, new HashSet<String>());
		}
		String storeName = StoreTemplate.TASKS_STORE_NAME;
		Iterator<DRow> taskRows = DBService.instance().getAllRowsAllColumns(storeName);
		while (taskRows.hasNext()) {
		    String rowKey = taskRows.next().getKey();
			if (!rowKey.startsWith(Defs.TASK_CLAIM_ROW_PREFIX + "/")) {
				int slashNdx = rowKey.indexOf('/');
				if (slashNdx > 0) {
					String appName = rowKey.substring(0, slashNdx);
					if (appNames.contains(appName)) {
						allTaskNames.get(appName).add(rowKey.substring(slashNdx + 1));
					}
				}
			}
		}
		
		return allTaskNames;
	}	// getAllTaskNames
	
	/**
	 * Returns all the task ids as they are defined in all the applications.
	 * 
	 * @return	Map&lt;appName, Set&lt;taskId&gt;&gt;
	 */
	public static Map<String, Set<String>> getAllDefinedTasks() {
		Map<String, Set<String>> allTaskNames = new HashMap<>();
        for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications()) {
        	Set<String> taskIds = new HashSet<>();
        	String appName = appDef.getAppName();
        	
        	// Loop through the schedule definitions
        	for (ScheduleDefinition schedDef : appDef.getSchedules().values()) {
        		SchedType schedType = schedDef.getType();
    			if (SchedType.APP_DEFAULT == schedType || SchedType.TABLE_DEFAULT == schedType) {
    				// We interested in real tasks only
    				continue;
    			} 
				String taskId = schedDef.getTableName();
				if (taskId == null) taskId = "*";
				taskId += '/' + schedType.getName();
				if (schedDef.getTaskDeclaration() != null) {
					taskId += '/' + schedDef.getTaskDeclaration();
				}
				taskIds.add(taskId);
        	}
        	allTaskNames.put(appName,  taskIds);
        }
        return allTaskNames;
	}	// getAllDefinedTasks
	
	/**
	 * Extracts task status from the database and returns it.
	 * If no record for the task exists, "Undefined" status is returned.
	 * 
	 * @param appName	Application name
	 * @param taskId	Task key
	 * @return			Task status (undefined if no status was actually found)
	 */
	public static TaskStatus getTaskStatus(String appName, String taskId) {
		String storeName = StoreTemplate.TASKS_STORE_NAME;
		Iterator<DColumn> colIter = null;
		TaskStatus status = new TaskStatus(TaskRunState.Undefined, -1, -1, -1, new HashMap<String, String>(), false, "0.0.0.0");
		try {
		    colIter = DBService.instance().getAllColumns(storeName, appName + "/" + taskId);
		} catch (Exception e) {
		}
		if (colIter == null) {
			return status;
		}
		while (colIter.hasNext()) {
		    DColumn col = colIter.next();
			long longValue = 0;
			switch (col.getName()) {
			case TaskStatus.STATUS_COL_NAME:
				status.setLastRunState(TaskRunState.statusFromString(col.getValue()));
				break;
			case TaskStatus.SCHEDULED_START_COL_NAME:
				try { longValue = Long.parseLong(col.getValue()); } catch (NumberFormatException e) {}
				status.setLastRunScheduledStartTime(longValue);
				break;
			case TaskStatus.ACTUAL_START_COL_NAME:
				try { longValue = Long.parseLong(col.getValue()); } catch (NumberFormatException e) {}
				status.setLastRunActualStartTime(longValue);
				break;
			case TaskStatus.ACTUAL_FINISH_COL_NAME:
				try { longValue = Long.parseLong(col.getValue()); } catch (NumberFormatException e) {}
				status.setLastRunFinishTime(longValue);
				break;
			case TaskStatus.SUSPENDED_COL_NAME:
				status.setSchedulingSuspended("true".equalsIgnoreCase(col.getValue()));
				break;
			case TaskStatus.EXECUTOR_COL_NAME:
				String host = col.getValue();
				status.setHostName(host);
				break;
			default:
				status.getLastRunDetails().put(col.getName(), col.getValue());
				break;
			}
		}
		return status;
	}	// getTaskStatus
	
	public static Set<TaskId> getLiveTasks() {
		return getLiveTasks(null);
	}
	
	/**
	 * Extracts all the states of the tasks that are registered in the Tasks table
	 * (hence were once started).
	 * 
	 * @param appName	Show only the tasks that belong a given application.
	 * 					If null - shows all tasks.
	 * @return	Map (taskId -> taskStatus) of the tasks in the Tasks table.
	 */
	public static Set<TaskId> getLiveTasks(String appName) {
		Set<TaskId> tasksSet = new HashSet<>();
		String tasksStore = StoreTemplate.TASKS_STORE_NAME;
    	Iterator<DRow> rowIter = DBService.instance().getAllRowsAllColumns(tasksStore);
    	while (rowIter.hasNext()) {
    	    DRow row = rowIter.next();
    	    String taskName = row.getKey();
    		if (taskName.startsWith(Defs.TASK_CLAIM_ROW_PREFIX + "/")) {
    			// There are "task" rows and "claim" rows in the table.
    			// We are interested in "task" rows only.
    			continue;
    		}
    		
    		if (appName != null && !taskName.startsWith(appName + "/")) {
    			// A task doesn't belong to the needed application
    			continue;
    		}
    		
    		TaskId taskId = TaskId.fromTaskTableId(taskName);
    		tasksSet.add(taskId);
    	}
		return tasksSet;
	}	// getLiveTasks
	
	/**
	 * Adds or updates task record for its status.
	 * 
	 * @param appName	Application name
	 * @param taskId	Task key
	 * @param status	Task status
	 */
	public static void setTaskStatus(String appName, String taskId, TaskStatus status) {
		String store = StoreTemplate.TASKS_STORE_NAME;
		String rowKey = appName + "/" + taskId;
		DBTransaction transaction = DBService.instance().startTransaction();
		transaction.addColumn(store, rowKey, TaskStatus.SCHEDULED_START_COL_NAME, status.getLastRunScheduledStartTime());
		transaction.addColumn(store, rowKey, TaskStatus.STATUS_COL_NAME, Utils.toBytes(status.getLastRunState().name()));
		transaction.addColumn(store, rowKey, TaskStatus.ACTUAL_FINISH_COL_NAME, status.getLastRunFinishTime());
		transaction.addColumn(store, rowKey, TaskStatus.ACTUAL_START_COL_NAME, status.getLastRunActualStartTime());
		transaction.addColumn(store, rowKey, TaskStatus.EXECUTOR_COL_NAME, Utils.toBytes(status.getHostName()));
		transaction.addColumn(store, rowKey, TaskStatus.SUSPENDED_COL_NAME, Utils.toBytes("" + status.isSchedulingSuspended()));
		DBService.instance().commit(transaction);
	}	// setTaskStatus
	
	/**
	 * Implements an algorithm of selecting a Doradus node which will execute
	 * a background task. The task instance is defined by task ID and task schedule time.
	 * 
	 * @param appName		Application name
	 * @param taskId		Task ID
	 * @param scheduledAt	Task schedule time
	 * @return				true if the claim was confirmed
	 */
	public static boolean claim(String appName, String taskId, long scheduledAt) {
		String store = StoreTemplate.TASKS_STORE_NAME;
		String rowKey = Defs.TASK_CLAIM_ROW_PREFIX + "/" + appName + "/" + taskId;
		String host = TaskManagerService.instance().getLocalHostAddress();
		String strScheduledAt = Long.toString(scheduledAt) + "/";
		
		// 1. Delay for several seconds for randomizing nodes (0 - task_exec_delay seconds)
		try {
			Thread.sleep(Math.round(s_rnd.nextDouble() * ServerConfig.getInstance().task_exec_delay * 1000));
		} catch (InterruptedException e) {
			// Waked up? OK, let's continue working
		}
		
		// 2. Check whether there were more claims from other nodes
		DBService dbService = DBService.instance();
		DBTransaction transaction = dbService.startTransaction();
		Iterator<DColumn> allColumns = dbService.getAllColumns(store, rowKey);
		if (allColumns != null) {
			boolean found = false;
			while (allColumns.hasNext()) {
			    DColumn col = allColumns.next();
				if (found = col.getName().startsWith(strScheduledAt) &&
						   !col.getName().endsWith('/' + host)) {
					break;
				} else {
					// 3. Delete old claims of the task for database space saving
					transaction.deleteColumn(store, rowKey, col.getName());
				}
			}
			// found a claim? no new claim will be added
			if (found) {
				transaction.clear();
				return false;
			} else {
				dbService.commit(transaction);
			}
		}
		
		// 4. Claim for the execution
		transaction = dbService.startTransaction();
		transaction.addColumn(store, rowKey, strScheduledAt + host, System.currentTimeMillis());
		dbService.commit(transaction);
		
		// 5. Waiting for task_exec_delay seconds
		try {
			Thread.sleep(Math.round(ServerConfig.getInstance().task_exec_delay * 1000));
		} catch (InterruptedException e) {
			// Waked up? OK, let's continue working
		}
		
		// 6. Select an earliest claim to see whether we won.
		Iterator<DColumn> allClaims = dbService.getAllColumns(store, rowKey);
		String earliestClaim = null;
		long earliestTime = Long.MAX_VALUE;
		while (allClaims.hasNext()) {
		    DColumn claim = allClaims.next();
			long claimTime = Long.parseLong(claim.getValue());
			if (claim.getName().startsWith(strScheduledAt) && 
					(claimTime < earliestTime || 
							(claimTime == earliestTime && 
							 claim.getName().compareTo(earliestClaim) < 0))) {
				earliestClaim = claim.getName();
				earliestTime = claimTime;
			}
		}
		
		
		return earliestClaim != null &&
			   earliestClaim.substring(strScheduledAt.length()).equals(host);
	}	// claim
	
	/**
	 * Creates or updates a task record to mark the task as "Started".
	 * It also sets started times, and stores a node IP address of the executor.
	 * 
	 * @param task	Task to start
	 */
	public static void startTask(DoradusTask task) {
		TaskStatus status = getTaskStatus(task.getAppName(), task.getTaskId());
		status.setLastRunScheduledStartTime(task.getScheduledTime());
		status.setLastRunActualStartTime(System.currentTimeMillis());
		status.setLastRunState(TaskRunState.Started);
		status.setHostName(TaskManagerService.instance().getLocalHostAddress());
		setTaskStatus(task.getAppName(), task.getTaskId(), status);
	}	// startTask
	
	/**
	 * Marks running task as "Succeeded" or "Interrupted" depending on how
	 * the task finished. Sets finish time.
	 * 
	 * @param task	Task to finish
	 */
	public static void finishTask(DoradusTask task) {
		TaskStatus status = getTaskStatus(task.getAppName(), task.getTaskId());
		status.setLastRunFinishTime(System.currentTimeMillis());
		status.setLastRunState(TaskRunState.statusFromString(task.getExecutingContext().getTaskExecutor().getStatusMessage()));
		setTaskStatus(task.getAppName(), task.getTaskId(), status);
	}	// finishTask
	
	/**
	 * Mark a task status as "Unknown". Usually it happens when a started Doradus node
	 * finds that there are tasks that were Started by the node, but never ended
	 * 
	 * @param task	task to make "Unknown"
	 */
	public static void invalidateTask(DoradusTask task) {
		TaskStatus status = getTaskStatus(task.getAppName(), task.getTaskId());
		status.setLastRunState(TaskRunState.Unknown);
		setTaskStatus(task.getAppName(), task.getTaskId(), status);
	}	// invalidateTask
	
	/**
	 * Marks a task as "Failed" due to unexpected exception during the task execution.
	 * 
	 * @param task	Task to mark as "Failed"
	 */
	public static void finishAbnormallyTask(DoradusTask task) {
		TaskStatus status = getTaskStatus(task.getAppName(), task.getTaskId());
		status.setLastRunFinishTime(System.currentTimeMillis());
		status.setLastRunState(TaskRunState.Failed);
		setTaskStatus(task.getAppName(), task.getTaskId(), status);
	}	// finishAbnormallyTask
	
	/**
	 * Marks task as "Interrupting" if it has "Started" status.
	 * Does nothing if the task doesn't exist.
	 * 
	 * @param appName	Application name
	 * @param taskId	Task name
	 */
	public static boolean interruptTask(String appName, String taskId) {
		TaskStatus status = getTaskStatus(appName, taskId);
		boolean interrupted = status.getLastRunState() == TaskRunState.Started;
		if (interrupted) {
			status.setLastRunState(TaskRunState.Interrupting);
			setTaskStatus(appName, taskId, status);
		}
		return interrupted;
	}	// interruptTask
	
	/**
	 * Marks task as "Suspended" to prevent it from following launches.
	 * If the task doesn't exist, creates a record for that task.
	 * 
	 * @param appName	Application name
	 * @param taskId	Task name
	 */
	public static void suspendTask(String appName, String taskId) {
		TaskStatus status = getTaskStatus(appName, taskId);
		status.setSchedulingSuspended(true);
		setTaskStatus(appName, taskId, status);
	}	// suspendTask
	
	/**
	 * Removes a "Suspended" flag from a task to allow following launches.
	 * Does nothing, if the task doesn't exist.
	 * 
	 * @param appName	Application name
	 * @param taskId	Task name
	 */
	public static void resumeTask(String appName, String taskId) {
		TaskStatus status = getTaskStatus(appName, taskId);
		if (status.isSchedulingSuspended()) {
			status.setSchedulingSuspended(false);
			setTaskStatus(appName, taskId, status);
		}
	}	// resumeTask
	
	/**
	 * Tests tasks records to find any tasks that do not belong to any existing
	 * application. The function is called once on TaskManagmentService start.
	 */
	public static void checkUnknownTasks() {
		String tasksStore = StoreTemplate.TASKS_STORE_NAME;
    	Iterator<DRow> rowIter = DBService.instance().getAllRowsAllColumns(tasksStore);
    	Set<String> unknownApps = new HashSet<String>();

    	while (rowIter.hasNext()) {
    	    String[] rowKeySections = rowIter.next().getKey().split("/");
    	    String appName = rowKeySections
    	            [Defs.TASK_CLAIM_ROW_PREFIX.equals(rowKeySections[0]) ? 1 : 0];
    		if (!unknownApps.contains(appName) &&
    				SchemaService.instance().getApplication(appName) == null) {
    			unknownApps.add(appName);
    		}
    	}
    	
    	for (String appName : unknownApps) {
    		deleteAppTasks(appName);
    	}
	}
	
	/**
	 * Tests tasks records to find any tasks that were started by this node but
	 * was never ended. The function is called once on TaskManagmentService start.
	 */
	public static void checkHangedTasks() {
		String host = TaskManagerService.instance().getLocalHostAddress();
		String tasksStore = StoreTemplate.TASKS_STORE_NAME;
		// Transaction for changing tasks status
		DBTransaction transaction = DBService.instance().startTransaction();
    	Iterator<DRow> rowIter = DBService.instance().getAllRowsAllColumns(tasksStore);
    	while (rowIter.hasNext()) {
    	    DRow row = rowIter.next();
    		if (row.getKey().startsWith(Defs.TASK_CLAIM_ROW_PREFIX + "/")) {
    			// There are "task" rows and "claim" rows in the table.
    			// We are interested in "task" rows only.
    			continue;
    		}
    		
    		boolean thisExecutor = false;	// task was executed by this node last time?
    		boolean stillActive = false;	// task is marked as still running?
    		Iterator<DColumn> colIter = row.getColumns();
    		while (colIter.hasNext()) {
    	        DColumn col = colIter.next();
    	        if (TaskStatus.EXECUTOR_COL_NAME.equals(col.getName()) &&
    	        		host.equals(col.getValue())) {
    	        	// Executor was this node
    	        	thisExecutor = true;
    	        } else if (TaskStatus.STATUS_COL_NAME.equals(col.getName()) &&
    	        		(TaskRunState.Started.name().equals(col.getValue()) || 
    	        		 TaskRunState.Interrupting.name().equals(col.getValue()))) {
    	        	// The task is still active
    	        	stillActive = true;
    	        }
    		}
    		if (thisExecutor && stillActive) {
    			transaction.addColumn(tasksStore, row.getKey(), TaskStatus.STATUS_COL_NAME,
    					Utils.toBytes(TaskRunState.Unknown.name()));
    		}
    	}
		DBService.instance().commit(transaction);
	}	// checkHangedTasks
	
	/**
	 * Prepares a migration from "old" tasks structure to a "new" (services) tasks structure.
	 * 
	 * @return	Information about tasks extracted from "old" structures.
	 */
	public static Map<String, Map<String, TaskStatus>> getAllOldTasks() {
		Olap olap = new Olap();
		Map<String, Map<String, TaskStatus>> map = new HashMap<>();
		List<ApplicationDefinition> allApps = SchemaService.instance().getAllApplications();
		for (ApplicationDefinition app : allApps) {
			String appName = app.getAppName();
			if ("OLAPService".equals(app.getStorageService())) {
				OlapTasks olapTasks = olap.getTasks(appName);
				Map<String, TaskStatus> tasksMap = olapTasks.getTasks();
				if (!tasksMap.isEmpty()) {
					for (TaskStatus task : tasksMap.values()) {
						task.setLastRunScheduledStartTime(task.getLastRunActualStartTime());
					}
					map.put(appName, tasksMap);
				}
				olapTasks.getTasks().clear();
				olap.saveTasks(appName, olapTasks);
			} else if (DBService.instance().storeExists(appName + "_Tasks")) {
				Iterator<DRow> iRows = DBService.instance().getAllRowsAllColumns(appName + "_Tasks");
				while (iRows.hasNext()) {
					DRow row = iRows.next();
					Map<String, TaskStatus> tasks = map.get(appName);
					if (tasks == null) {
						map.put(appName, tasks = new HashMap<String, TaskStatus>());
					}
					TaskStatus taskStatus = new TaskStatus();
					Iterator<DColumn> iColumns = row.getColumns();
					while (iColumns.hasNext()) {
						DColumn column = iColumns.next();
						switch (column.getName()) {
						case TaskStatus.DEFAULT_DB_STATUS:
							taskStatus.setLastRunState(TaskRunState.statusFromString(column.getValue()));
							break;
						case TaskStatus.DEFAULT_DB_STARTTIME:
							long time = Utils.toLong(column.getRawValue());
							taskStatus.setLastRunActualStartTime(time);
							taskStatus.setLastRunScheduledStartTime(time);
							break;
						case TaskStatus.DEFAULT_DB_FINISHTIME:
							taskStatus.setLastRunFinishTime(Utils.toLong(column.getRawValue()));
							break;
						}
					}
					tasks.put(row.getKey(), taskStatus);
				}
				DBService.instance().deleteStoreIfPresent(appName + "_Tasks");
			}
		}
		return map;
	}	// getAllOldTasks

	/**
	 * Deletes information about tasks of a given application.
	 * 
	 * @param appName	Application name
	 */
	public static void deleteAppTasks(String appName) {
		DBTransaction transaction = DBService.instance().startTransaction();
		String tasksStore = StoreTemplate.TASKS_STORE_NAME;
		Iterator<DRow> rowIter = DBService.instance().getAllRowsAllColumns(tasksStore);
		while (rowIter.hasNext()) {
			DRow nextRow = rowIter.next();
			String rowKey = nextRow.getKey();
			if (rowKey.startsWith(appName + "/") ||
				rowKey.startsWith(Defs.TASK_CLAIM_ROW_PREFIX + "/" + appName + "/")) {
				transaction.deleteRow(tasksStore, rowKey);
			}
		}
		DBService.instance().commit(transaction);
	}	// deleteAppTasks
}
