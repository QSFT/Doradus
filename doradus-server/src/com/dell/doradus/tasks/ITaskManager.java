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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.management.TaskSettings;
import com.dell.doradus.management.TaskStatus;



/**
 * Specifies the task management API. In network protocol-independent manner, 
 * this API defines functionality which we want to present to remote JMX/REST  
 * clients for managing and monitoring of background tasks.
 */
public interface ITaskManager {
	// Application types
	public static final String OLAP_PREFIX_NAME = "olap";
	public static final String OLAP_PREFIX = OLAP_PREFIX_NAME + ":";
		
	/**
	 * Gets the names of all currently existent applications.
	 * @return Set<String>
	 * @throws IOException 
	 */
	Set<String> getAppNames() throws IOException;
	
	/**
	 * Gets the default settings for all tasks in all all applications.
	 * (I assume the global settings are defined in Doradus configuration).
	 * 
	 * @return TaskSettings
	 */
	TaskSettings getGlobalDefaultSettings();
		
	/**
	 * Gets the collection of TaskSettings objects which represents hierarchy of 
	 * all tasks and groups of tasks which belong to named application..
	 * 
	 * @param appName The application name.
	 * @return Map<String,TaskSettings>, where each key is a task/group-key 
	 * 		equal to the getKey() value returned by the corresponding TaskSettings 
	 * 		value.
	 * @throws IOException 
	 */
	Map<String,TaskSettings> getAppSettings(String appName) throws IOException;	
	
	/**
	 * Gets the current status of identified task belonging to named application.
	 * 
	 * @param appName The application name.
	 * @param taskKey The key provided by the TaskSettings object that represents the task.
	 * @return TaskStatus
	 */
	TaskStatus getTaskStatus(String appName, String taskKey);
	
	/**
	 * Sends the "interrupt execution" signal to each currently executing task.
	 * Reaction of each task to such signal depends on the task type.
	 */
	void interrupt() throws IOException;
	
	/**
	 * Suspends the scheduling of starts of all tasks. If scheduling of any 
	 * of the tasks is already suspended then ignores this task.
	 * <p>
	 * Execution of already started tasks proceeds without changes.
	 * @throws IOException 
	 */
	void suspend() throws IOException;
	
	/**
	 * Resumes the scheduling of starts of all tasks. If scheduling of any 
	 * of the tasks has not been suspended then ignores this task.
	 * @throws IOException 
	 */
	void resume() throws IOException;
	
	/**
	 * Sends the "interrupt execution" signal to each currently executing 
	 * task belonging to named application.
	 * 
	 * @param appName The application name.
	 * @throws IOException 
	 */
	void interrupt(String appName) throws IOException;
	
	/**
	 * Suspends the scheduling of starts of all tasks belonging to named 
	 * application. If scheduling of any of the tasks is already suspended 
	 * then ignores this task.
	 * <p>
	 * Execution of already started tasks proceeds without changes.
	 * @throws IOException 
	 */
	void suspend(String appName) throws IOException;
	
	/**
	 * Resumes the scheduling of starts of all tasks belonging to named 
	 * application. If scheduling of any of the tasks has not been suspended 
	 * then ignores this task.
	 * 
	 * @param appName The application name.
	 * @throws IOException 
	 */
	void resume(String appName) throws IOException;
	
	/**
	 * Sends the "interrupt execution" signal to identified task or group of tasks
	 * belonging to named application. It is empty operation for tasks which are not 
	 * executing now.
	 * 
	 * @param appName The application name.
	 * @param taskOrGroupKey The key provided by the TaskSettings object that 
	 * 		represents a task or group.
	 * @return True if at least one task was found to interrupt, false otherwise.
	 */
	boolean interrupt(String appName, String taskOrGroupKey);
	
	/**
	 * Suspends the scheduling of starts of identified task or group of tasks 
	 * belonging to named application. It is empty operation for tasks the scheduling 
	 * of which is already suspended.
	 * <p>
	 * Already started tasks proceed without changes.
	 * 
	 * @param appName The application name.
	 * @param taskOrGroupKey The key provided by the TaskSettings object that 
	 * 		represents a task or group.
	 */
	void suspend(String appName, String taskOrGroupKey);
	
	/**
	 * Resumes the scheduling of starts of identified task or group of tasks
	 * belonging to named application. It is empty operation for tasks the scheduling
	 * of which has not been suspended.
	 * 
	 * @param appName The application name.
	 * @param taskOrGroupKey The key provided by the TaskSettings object that 
	 * 		represents a task or group.
	 * @throws IOException 
	 */
	void resume(String appName, String taskOrGroupKey) throws IOException;
	
	/**
	 * Halts the scheduling of all tasks, and cleans up all resources associated 
	 * with this manager. 
	 * <p>
	 * The manager cannot be re-started. 
	 * 
	 * @param waitForTasksToComplete Set true to disallow this method to return 
	 * until all currently executing tasks have completed. 
	 * @throws IOException 
	 */
	void shutdown(boolean waitForTasksToComplete) throws IOException;
	
	/**
	 * Updates the settings of concrete task or group of tasks belonging to named 
	 * application.
	 * <p>
	 * (I think, if a task is in started state then we must allow to complete this run 
	 * in normal way). 
	 * 
	 * @param appName The application name.
	 * @param settings TaskSettings object containing new values of settings.
	 * @throws IOException 
	 */
	void updateSettings(String appName, TaskSettings settings) throws IOException;
	
	/**
	 * Refreshes the table of the tasks schedule. The method should be called
	 * when the schema is updated (by REST request or by any other reason)
	 * @throws IOException 
	 */
	void startScheduling() throws IOException;
	
	/**
	 * Extracts current task status from the database. It is not necessary the
	 * actual task status, but only the last information about the task as it
	 * was last time saved in the database.
	 * @param appName Application
	 * @param taskName Task Name
	 * @return
	 */
	TaskStatus getTaskInfo(String appName, String taskName);
	
	/**
	 * Stores current task status into the database. The status is changed
	 * every time when a task is started, finished, or interrupted.
	 * @param appName
	 * @param taskName
	 * @param status
	 */
	void setTaskInfo(String appName, String taskName, TaskStatus status);
	
	/**
	 * Starts the task once immediately in addition to its general schedule.
	 * @param appName Application name
	 * @param taskId Task identifier
	 * @return True if the task is found and can be started, False otherwise.
	 * @throws IOException 
	 */
	boolean startImmediately(String appName, String taskId) throws IOException;
}
