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

package com.dell.doradus.mbeans;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.management.LongJob;
import com.dell.doradus.management.StorageManagerMXBean;
import com.dell.doradus.management.TaskSettings;
import com.dell.doradus.management.TaskStatus;

/**
 * Implements the StorageManagerMXBean interface to the Cassandra database node
 * specified by configuration settings of running <i>Doradus</i>-server instance.
 * <p>
 * <b>NOTE:</b> The constructors of this class is not intended for direct usage.
 * Instead, use {@code getStorageManager} static method of the
 * {@code MBeanProvider} class.
 */
public class StorageManager extends MBeanBase implements StorageManagerMXBean {
	private Logger logger = LoggerFactory.getLogger(getClass().getSimpleName());
	static int JOB_LIST_MAX_SIZE = 100;

	/**
	 * Creates new StorageManager instance and optionally registers it on the
	 * platform MBeanServer.
	 * 
	 * @param publish
	 *            The true, if you want to register the created instance.
	 *            Otherwise, nonpublic bean will be constructed.
	 */
	public StorageManager(boolean publish) {
		this.domain = JMX_DOMAIN_NAME;
		this.type = JMX_TYPE_NAME;

		if (publish) {
			register();
		}
	}
	
    /**
     * Blocks until the active asynchronous operation (-"long job") completed 
     * execution or the timeout occurs.
     *
     * @param timeoutInSeconds the maximum time to wait, in seconds
     * @param unit the time unit of the timeout argument
     * @return <tt>true</tt> if job terminated and <tt>false</tt> 
     * 		if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting.
     */
    public synchronized boolean awaitJobTermination(long timeoutInSeconds) throws InterruptedException {
		if (future != null && !future.isDone()) {
			logger.info("Blocking until active job completed (timeout: " + timeoutInSeconds + " secs).");
			return executor.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS);
		}
    	return true;
    }


	/**
	 * The operation mode of the Cassandra node.
	 * 
	 * @return one of: NORMAL, CLIENT, JOINING, LEAVING, DECOMMISSIONED, MOVING,
	 *         DRAINING, or DRAINED.
	 */
	@Override
	public String getOperationMode() {
		String mode = getNode().getOperationMode();
		if(mode != null && NORMAL.equals(mode.toUpperCase())) {
			mode = NORMAL;
		}
		return mode;
	}
	
	@Override
	public String getReleaseVersion() {
		return getNode().getReleaseVersion();
	}

	
	/**
	 * @return <os-name>, <arch>
	 */
	public String getOS() {
		return getNode().getOS();
	}

	
	/**
	 * @return {@code FOREGROUND}, {@code BACKGROUND}, or {@code UNKNOWN}
	 */
	public int getStartMode() {
		try {
			return getNode().isInForeground() ? FOREGROUND : BACKGROUND;
		} catch(Exception ex) {
			return UNKNOWN;
		}
	}

	/**
	 * The descriptor of asynchronous operation (-"long job") which this manager
	 * either executes currently or has executed last.
	 * 
	 * @return The LongJob instance or null if there were no requests of such
	 *         operations.
	 */
	@Override
	public LongJob getRecentJob() {
		return recentJob;
	}
	
	/**
	 * Gets the descriptor of asynchronous operation (-"long job") which this manager
	 * either executes currently or has executed earlier.
	 * @param jobID The ID of an asynchronous operation. 
	 * @return The LongJob instance or null, if ID is unknown in this manager.
	 */
	@Override
	public LongJob getJobByID(String jobID) {
		return jobMap.get(jobID);
	}
	
	/**
	 * Enumerates all asynchronous operations which this manager
	 * either executes currently or has executed earlier.
	 * @return The mapping of job ID to the job name.
	 */
	public String[] getJobList() {
		return (String[])jobList.toArray(new String[0]);
	}

	/**
	 * The names list of data snapshots which exist in the local (!) storage
	 * node.
	 * 
	 * @return a String[] of snapshot names.
	 * @exception java.lang.IllegalStateException
	 *                if storage node is not local.
	 */
	@Override
	public String[] getSnapshotList() {
		return getNode().getSnapshotList();
	}
	
	/**
	 * Returns the total number of nodes in database cluster. Zero is returned if unavailable.
	 * @return
	 */
	public int getNodesCount() {
		return getNode().getNodesCount();
	}

	/**
	 * Starts the asynchronous operation (- "long job") that will create new
	 * data snapshot. The operation will be completed with FAILED status if
	 * storage node is not in NORMAL mode or specified snapshot already exists.
	 * 
	 * @param snapshotName
	 *            The name of snapshot.
	 * @exception java.lang.IllegalStateException
	 *                if other job is executing currently.
	 */
	@Override
	public String startCreateSnapshot(String snapshotName) {
		String jobID = consNextJobID();
		String jobName = consJobName("createBackup", snapshotName);
		LongJob job = getNode().consCreateSnapshotJob(jobID, jobName, snapshotName);
		startJob(job);
		return jobID;
	}

	/**
	 * Starts the asynchronous operation (- "long job") that will delete
	 * specified data snapshot. It is empty job if specified snapshot does not
	 * exist. The operation will be completed with FAILED status if storage node
	 * is not in NORMAL mode.
	 * 
	 * @param snapshotName
	 *            The name of snapshot.
	 * @exception java.lang.IllegalStateException
	 *            If other job is executing currently.
	 */
	@Override
	public String startDeleteSnapshot(String snapshotName) {
		String jobID = consNextJobID();
		String jobName = consJobName("deleteBackup", snapshotName);
		LongJob job = getNode().consDeleteSnapshotJob(jobID, jobName, snapshotName);
		startJob(job);
		return jobID;
	}

	/**
	 * Starts the asynchronous operation (- "long job") that will restore the
	 * data of local (!) storage node from specified snapshot. The operation
	 * will be completed with FAILED status if storage node is not local,
	 * storage node is not in NORMAL mode, or specified snapshot does not exist.
	 * 
	 * <p>
	 * WARN: This operation will shut down and restart the storage node using
	 * the "doradus-cassandra.bat" utility tool (- STOP and START commands).
	 * That is, it is supposed that 1) <i>Cassandra</i> works as a Windows
	 * service, and 2) the running <i>Doradus</i>-server has rights of Windows
	 * administrator.
	 * 
	 * @param snapshotName
	 *            The name of snapshot.
	 * @exception java.lang.IllegalStateException
	 *                If other job is executing currently.
	 */
	@Override
	public String  startRestoreFromSnapshot(String snapshotName) {
		String jobID = consNextJobID();
		String jobName = consJobName("restoreBackup", snapshotName);
		LongJob job = getNode().consRestoreFromSnapshotJob(jobID, jobName,
				snapshotName);
		startJob(job);
		return jobID;
	}

	
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	
//	 public String  sendInterruptTaskCommand(String appName, String taskOrGroupKey) {
//		String jobID = consNextJobID();
//		String jobName = consJobName("interruptTaskExecution", appName, taskOrGroupKey);
//		LongJob job = getTaskManager().consInterruptJob(jobID, jobName,
//				appName, taskOrGroupKey);
//		startJob(job);
//		return jobID;
//	 }
//	 public String  sendSuspendSchedulingCommand(String appName, String taskOrGroupKey) {
//			String jobID = consNextJobID();
//			String jobName = consJobName("suspendTaskScheduling", appName, taskOrGroupKey);
//			LongJob job = getTaskManager().consSuspendJob(jobID, jobName,
//					appName, taskOrGroupKey);
//			startJob(job);
//			return jobID;
//	 }
//	 public String  sendResumeSchedulingCommand(String appName, String taskOrGroupKey) {
//			String jobID = consNextJobID();
//			String jobName = consJobName("resumeTaskScheduling", appName, taskOrGroupKey);
//			LongJob job = getTaskManager().consResumeJob(jobID, jobName,
//					appName, taskOrGroupKey);
//			startJob(job);
//			return jobID;
//	 }
//	 public String  sendUpdateSettingsCommand(String appName, TaskSettings taskOrGroupSettings) {
//			String jobID = consNextJobID();
//			String jobName = consJobName("updateTaskSettings", appName, taskOrGroupSettings == null ? "null" : taskOrGroupSettings.getKey());
//			LongJob job = getTaskManager().consUpdateJob(jobID, jobName,
//					appName, taskOrGroupSettings);
//			startJob(job);
//			return jobID;
//	 }
//	 public TaskSettings getGlobalDefaultSettings() {
//		 return getTaskManager().getGlobalDefaultSettings();
//	 }
//	 public Set<String> getAppNames() {
//		 return getTaskManager().getAppNames();
//	 }
//	 public Map<String, TaskSettings> getAppSettings(String appName) {
//		 return getTaskManager().getAppSettings(appName);
//	 }
//	 public TaskStatus getTaskStatus(String appName, String taskKey) {
//		 return getTaskManager().getTaskStatus(appName, taskKey);
//	 }
	 

	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	private String consJobName(String opName, String... args) {
		StringBuilder b = new StringBuilder();
		b.append(opName + "(");
		for(int i = 0; i < args.length; i++) {
			if(i > 0) b.append(", ");
			b.append(args[i]);
		}
		b.append(")");
		return b.toString();
	}

	private CassandraNode getNode() {
		if (node == null) {
			synchronized (this) {
				if (node == null) {
					ServerConfig c = ServerConfig.getInstance();
					node = new CassandraNode(c.dbhost, c.jmxport);
				}
			}
		}
		return node;
	}
	
	private TaskManagerAdapter getTaskManager() {
		if (taskManager == null) {
			synchronized (this) {
				if (taskManager == null) {
					taskManager = new TaskManagerAdapter();
				}
			}
		}
		return taskManager;
	}

	private synchronized void startJob(LongJob job) {
		if (future != null && !future.isDone()) {
			throw new IllegalStateException("Service busy. The \""
					+ recentJob.getName() + "\" job is in progress.");
		}

		if (executor == null) {
			executor = Executors.newSingleThreadExecutor();
		}

		
		if(jobList.size() >= JOB_LIST_MAX_SIZE) {
			String firstId = jobList.get(0);
			jobList.remove(0);
			jobMap.remove(firstId);
		}

		recentJob = job;	
		jobMap.put(job.getId(), job);
		jobList.add(job.getId());
		
		future = executor.submit(job);
	}
	
	private synchronized String consNextJobID() {
		if(pid == null) {
			pid = getProcessId();
		}
	
		return pid + "-" + (++jobCount);
	}
	
	private String getProcessId() {
	    // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
	    final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
	    final int index = jvmName.indexOf('@');
	    
	    if(index >= 1) {
		    try {
		        return Long.toString(Long.parseLong(jvmName.substring(0, index)));
		    } catch (NumberFormatException e) {
		    }
	    }
	    
	    logger.warn("Can't get current process id. Instead, using the JVM name: " + jvmName);
	    return jvmName;
	}

	private static int jobCount;
	private CassandraNode node;
	private LongJob recentJob;
	private ExecutorService executor;
	private Future<?> future;
	private String pid;
	private Map<String,LongJob> jobMap = new HashMap<String,LongJob>();
	private List<String> jobList = new LinkedList<String>();
	private TaskManagerAdapter taskManager;
}
