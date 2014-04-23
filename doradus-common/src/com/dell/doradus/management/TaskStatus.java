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

import java.beans.ConstructorProperties;
import java.util.Date;
import java.util.Map;

import com.dell.doradus.common.UNode;

/**
 * Each TaskStatus is object where the task registers the changes in own state,
 * and the task execution environment saves information about external control 
 * actions with this task.
 * <p>
 * Some properties of TaskStatus must be persistent 1) to allow the scheduler 
 * to properly schedule task execution even after the Doradus restart, and
 * 2) to allow the administrator to see a real order of the events. 
 * (Look for "@category Persistent" in comments to getters of this class).
 */
public class TaskStatus {
	
	 public final static String DEFAULT_DB_STATUS = "Status";
	 public final static String DEFAULT_DB_STARTTIME = "LastPerformed";
	 public final static String DEFAULT_DB_FINISHTIME = "FinishTime";
	 public final static String DEFAULT_DB_ELAPSEDTIME = "ElapsedTimeSeconds";

	 public final static String SCHEDULED_START_COL_NAME = "ScheduledStart";
	 public final static String ACTUAL_START_COL_NAME = "LastPerformed";
	 public final static String ACTUAL_FINISH_COL_NAME = "FinishTime";
	 public final static String STATUS_COL_NAME = "Status";
	 public final static String EXECUTOR_COL_NAME = "Executor";
	 public final static String SUSPENDED_COL_NAME = "Suspended";
	 
	/**
	 * Constructs the TaskStatus object that will represent initial status of task.
	 */
	public TaskStatus() {
		this(TaskRunState.Undefined, -1, -1, -1, null, false);
	}

	/**
	 * Reconstructs the TaskStatus object, using the saved values of its persistent 
	 * properties.
	 * 
	 * @param lastRunState
	 * @param lastRunScheduledStartTime
	 * @param lastRunActualStartTime
	 * @param lastRunFinishTime
	 * @param lastRunDetails
	 */
	public TaskStatus(TaskRunState lastRunState,
			long lastRunScheduledStartTime, long lastRunActualStartTime,
			long lastRunFinishTime, Map<String, String> lastRunDetails) {
		this(lastRunState, lastRunScheduledStartTime, lastRunActualStartTime,
				lastRunFinishTime, lastRunDetails, false);
	}

	/**
	 * This constructor is not intended for direct usage. It is required to JMX
	 * for (de)serialization of the TaskStatus objects.
	 */
	@ConstructorProperties({ "lastRunState", "lastRunScheduledStartTime",
			"lastRunActualStartTime", "lastRunFinishTime", "lastRunDetails",
			"schedulingSuspended" })
	public TaskStatus(TaskRunState lastRunState,
			long lastRunScheduledStartTime, long lastRunActualStartTime,
			long lastRunFinishTime, Map<String, String> lastRunDetails,
			boolean schedulingSuspended) {
		this(lastRunState, lastRunScheduledStartTime, lastRunActualStartTime,
				lastRunFinishTime, lastRunDetails, schedulingSuspended, null);
	}


	public TaskStatus(TaskRunState lastRunState,
			long lastRunScheduledStartTime, long lastRunActualStartTime,
			long lastRunFinishTime, Map<String, String> lastRunDetails,
			boolean schedulingSuspended, String host) {
		this.state = lastRunState;
		this.scheduledStartTime = lastRunScheduledStartTime;
		this.actualStartTime = lastRunActualStartTime;
		this.finishTime = lastRunFinishTime;
		this.details = lastRunDetails;
		this.suspended = schedulingSuspended;
		this.hostName = host;
	}

	/**
	 * Gets the task's last run state.
	 * 
	 * @category Persistent property.
	 * @return RunStatus
	 */
	public TaskRunState getLastRunState() {
		return state;
	}
	
	/**
	 * 
	 * @param state
	 */
	public void setLastRunState(TaskRunState state) {
		this.state = state;
	}

	/**
	 * Gets the scheduled time when the last run should be started.
	 * <p>
	 * NOTE: As we rely on default schedules, a situation will be typical when two 
	 * or more tasks shall begin execution at the same time. I think to avoid such 
	 * situations, we will want to shift, somehow, real start time of tasks, after the 
	 * scheduler's triggers have been already fired. As the result,  scheduled and 
	 * actual time of start of any task can differ essentially.
	 * 
	 * @category Persistent property.
	 * @return Time, in milliseconds, or -1 if never.
	 */
	public long getLastRunScheduledStartTime() {
		return scheduledStartTime;
	}
	
	/**
	 * 
	 * @param scheduledStartTime
	 */
	public void setLastRunScheduledStartTime(long scheduledStartTime) {
		this.scheduledStartTime = scheduledStartTime < 0 ? -1 : scheduledStartTime;
	}

	/**
	 * Gets the actual time when the last run of task was started
	 * (see getLastRunScheduledStartTime for comments).
	 * 
	 * @category Persistent property.
	 * @return Time, in milliseconds, or -1 if never.
	 */
	public long getLastRunActualStartTime() {
		return actualStartTime;
	}
	
	/**
	 * 
	 * @param actualStartTime
	 */
	public void setLastRunActualStartTime(long actualStartTime) {
		this.actualStartTime = actualStartTime < 0 ? -1 : actualStartTime;
	}

	/**
	 * Gets the time when the last run of task was finished.
	 * 
	 * @category Persistent property.
	 * @return Time, in milliseconds, if isLastRunCompleted() is true. 
	 * 	Otherwise, -1.
	 */
	public long getLastRunFinishTime() {
		return finishTime;
	}
	
	/**
	 * 
	 * @param finishTime
	 */
	public void setLastRunFinishTime(long finishTime) {
		this.finishTime = finishTime < 0 ? -1 : finishTime;
	}

	/**
	 * Gets the task-type specific properties of the last run of task. Keys and
	 * values must be readable strings.
	 * <p>
	 * (For example, if the last run failed then the details can be used to report 
	 * the reason).
	 * 
	 * @category Persistent property.
	 * @return Map<String,String> or null.
	 */
	public Map<String, String> getLastRunDetails() {
		return details;
	}
	
	/**
	 * 
	 * @param details
	 */
	public void setLastRunDetails(Map<String, String> details) {
		this.details = details;
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	/**
	 * Reports the task is executing just now.
	 * 
	 * @category Transient property.
	 * @return The true if last run is in Started or Interrupting state.
	 */
	public boolean isExecuting() {
		return state == TaskRunState.Started ||  state == TaskRunState.Interrupting;
	}

	/**
	 * Reports whether scheduling of task's starts has been suspended or not.
	 * (see ITaskManager.suspend).
	 * 
	 * @category Transient property.
	 * @return boolean
	 */
	public boolean isSchedulingSuspended() {
		return suspended;
	}
	
	/**
	 * 
	 * @param suspended
	 */
	public void setSchedulingSuspended(boolean suspended) {
		this.suspended = suspended;
	}
	
	/**
     * Compares this instance to the specified object.  The result is {@code
     * true} if and only if the argument is not {@code null} and is a {@code
     * TaskStatus} object that contains the same values of properties as 
     * this object.
	 */
	@Override
	public boolean equals(Object obj) {
		if(this == obj)
			return true;
		
		if(obj instanceof TaskStatus) {
			TaskStatus s = (TaskStatus)obj;
			boolean yes = (state == s.state
					&& scheduledStartTime == s.scheduledStartTime
					&& actualStartTime == s.actualStartTime
					&& finishTime == s.finishTime
					&& suspended == s.suspended);
			
			return yes && ((details == null && s.details == null) 
					|| (details != null && details.equals(s.details)));
		}
		
		return false;
	}


	protected TaskRunState state;
	protected long scheduledStartTime;
	protected long actualStartTime;
	protected long finishTime;
	protected Map<String, String> details;
	protected boolean suspended;
	protected String hostName;

	private final static String SUSPENDED_TAG = "suspended"; 
	private final static String SCHEDULE_TAG = "schedule"; 
	private final static String LAST_FINISH_TAG = "last-finished-time"; 
	private final static String LAST_START_TAG = "last-started-time"; 
	private final static String LAST_SCHEDULED_TAG = "last-scheduled-time"; 
	private final static String STATE_TAG = "state"; 
	 
	
	public String toStr() {
		StringBuilder b = new StringBuilder();
		b.append("schedulingSuspended:        " + suspended + "\n");
		b.append("lastRunState:               " + state + "\n");
		b.append("lastRunScheduledStartTime:  " + scheduledStartTime + "\n");
		b.append("lastRunActualStartTime:     " + actualStartTime + "\n");
		b.append("lastRunFinishTime:          " + finishTime + "\n");
		return b.toString();
	}

	public UNode toDoc(String taskId, String cronSchedule) {
	    UNode result = UNode.createMapNode(taskId, "task");
        result.addValueNode(STATE_TAG, state.name());
        if (scheduledStartTime != -1) {
            result.addValueNode(LAST_SCHEDULED_TAG, new Date(scheduledStartTime).toString());
        }
        if (actualStartTime != -1) {
            result.addValueNode(LAST_START_TAG, new Date(actualStartTime).toString());
        }
        if (finishTime != -1) {
            result.addValueNode(LAST_FINISH_TAG, new Date(finishTime).toString());
        }
        if (cronSchedule != null) {
            result.addValueNode(SCHEDULE_TAG, cronSchedule);
        }
        if (suspended) {
            result.addValueNode(SUSPENDED_TAG, "true");
        }
	    return result;
	}
}
