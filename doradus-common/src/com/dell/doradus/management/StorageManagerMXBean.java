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

package com.dell.doradus.management;

import java.util.Map;
import java.util.Set;


/**
 * Defines interface of a MXBean that implements the management operations with
 * database server (-"node") that is linked to the running <i>Doradus</i>-server instance. 
 * Location of this node is specified by the server's configuration settings.
 * 
 * <p>
 * The implementing class must perform long-term operations in asynchronous
 * mode, no more one operation at the same time. Clients will watch for value of
 * {@code RecentJob} attribute to track the progress of active job.
 * 
 * <p>
 * WARN: Some operations can be performed if only the server and database node
 * are hosted on the same machine.
 */
public interface StorageManagerMXBean {

	public static final String JMX_DOMAIN_NAME = "com.dell.doradus";
	public static final String JMX_TYPE_NAME = "StorageManager";
	
	public static final int UNKNOWN = 0;
	public static final int FOREGROUND = 1;
	public static final int BACKGROUND = 2;
	
	public static String NORMAL = "NORMAL";
	
	/**
	 * @return <os-name>, <arch>
	 */
	String getOS();
	
	/**
	 * @return Doradus server release version.
	 */
	String getReleaseVersion();

	/**
	 * The operation mode of the storage node. The {@code NORMAL} value indicates that
	 * operations with snapshots are enabled.
	 * 
	 * @return The operation mode of the storage node.
	 */
	String getOperationMode();
	
	/**
	 * @return {@code FOREGROUND}, {@code BACKGROUND}, or {@code UNKNOWN}
	 */
	int getStartMode();

	/**
	 * The descriptor of asynchronous operation (-"long job") which this manager
	 * either executes currently or has executed last.
	 * 
	 * @return The LongJob instance or null.
	 */
	LongJob getRecentJob();
	
	/**
	 * Gets the descriptor of asynchronous operation (-"long job") which this manager
	 * either executes currently or has executed earlier.
	 * @param jobID The ID of an asynchronous operation. 
	 * @return The LongJob instance or null, if ID is unknown in this manager.
	 */
	LongJob getJobByID(String jobID);
	
	/**
	 * Enumerates all asynchronous operations (-"long jobs") which this manager
	 * either executes currently or has executed earlier.
	 * @return The array of job IDs sorted in chronological order of the jobs.
	 */
	String[] getJobList();

	/**
	 * The names list of data snapshots which exist in the local (!) storage
	 * node. Exception will be thrown if storage node is not local.
	 * 
	 * @return a String[] of snapshot names.
	 */
	String[] getSnapshotList();
	
	/**
	 * Returns the total number of nodes in database cluster. Zero is returned if unavailable.
	 * @return The total number of nodes in database cluster.
	 */
	int getNodesCount();

	/**
	 * Starts the asynchronous operation (- "long job") that will create new
	 * data snapshot. The operation will be completed with FAILED status if
	 * specified snapshot already exists.
	 * 
	 * @param snapshotName
	 *            The name of snapshot.
	 * @exception java.lang.IllegalStateException
	 *                If other job is executing currently.
	 * @return The job ID.
	 */
	String startCreateSnapshot(String snapshotName);

	/**
	 * Starts the asynchronous operation (- "long job") that will delete
	 * specified data snapshot. It is empty job if specified snapshot does not
	 * exist.
	 * 
	 * @param snapshotName
	 *            The name of snapshot.
	 * @exception java.lang.IllegalStateException
	 *                If other job is executing currently.
	 * @return The job ID.
	 */
	String startDeleteSnapshot(String snapshotName);

	/**
	 * Starts the asynchronous operation (- "long job") that will restore the
	 * data of local (!) storage node from specified snapshot. The operation
	 * will be completed with FAILED status if specified snapshot does not
	 * exist.
	 * 
	 * <p>
	 * WARN: This operation will shut down and restart the storage node.
	 * 
	 * @param snapshotName
	 *            The name of snapshot.
	 * @exception java.lang.IllegalStateException
	 *                If other job is executing currently.
	 * @return The job ID.
	 */
	String startRestoreFromSnapshot(String snapshotName);
	
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	
//	String  sendInterruptTaskCommand(String appName, String taskOrGroupKey);
//	String  sendSuspendSchedulingCommand(String appName, String taskOrGroupKey);
//	String  sendResumeSchedulingCommand(String appName, String taskOrGroupKey);
//	String  sendUpdateSettingsCommand(String appName, TaskSettings taskOrGroupSettings);
//	TaskSettings getGlobalDefaultSettings();
//	Set<String> getAppNames();
//	Map<String, TaskSettings> getAppSettings(String appName);
//	TaskStatus getTaskStatus(String appName, String taskKey);

}
