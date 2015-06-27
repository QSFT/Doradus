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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.taskmanager.TaskRecord.TaskStatus;

/**
 * Abstract root class for tasks that can be executed by the {@link TaskManagerService}.
 * Each object identifies task details such as application, table (if applicable), task
 * name, and execution frequency. Subclasses must implement {@link #execute()}, which is
 * called when the task becomes ready to run by this process's task manager. 
 */
public abstract class Task implements Runnable {
    // Protected logger available to concrete services:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    // Fixed parameters from constructor:
    protected final ApplicationDefinition m_appDef;
    protected final String m_appName;
    protected final Tenant m_tenant;
    protected final String m_tableName;
    protected final String m_taskName;
    protected final TaskFrequency m_taskFreq;
    
    // Parameters set while executing:
    protected String m_hostID;
    protected TaskRecord m_taskRecord;
    protected long m_lastProgressTimestamp;
    protected String m_progressMessage;
    
    /**
     * Create a task with the given properties.
     * 
     * @param appDef        {@link ApplicationDefinition} of application for which task
     *                      belongs.
     * @param tableName     Name of the table to which the task applies. Can be empty or
     *                      null if the task is not table-specific.
     * @param taskName      Task identify that must be unique among all task types used
     *                      by a given storage manager. Example: "data-aging".
     * @param taskFreq      {@link TaskFrequency} in display format that describes how
     *                      often the task should be executed: "1 DAY", "30 MINUTES", etc.
     *                      Can be empty/null when the task is being executed "now".
     */
    public Task(ApplicationDefinition appDef, String tableName, String taskName, String taskFreq) {
        m_appDef = appDef;
        m_appName = appDef.getAppName();
        m_tenant = Tenant.getTenant(m_appDef);
        m_tableName = Utils.isEmpty(tableName) ? "*" : tableName;
        m_taskName = taskName;
        m_taskFreq = new TaskFrequency(Utils.isEmpty(taskFreq) ? "1 MINUTE" : taskFreq);
    }

    /**
     * Set the execution parameters for this task. This method is called by the
     * {@link TaskManagerService} immediately after constructing the job object.
     *  
     * @param hostID        Identifier of the executing node (IP address).
     * @param taskRecord    {@link TaskRecord} containing the task's current execution
     *                      properties from the Tasks table. This record is updated and
     *                      persisted as task execution proceeds.
     */
    void setParams(String hostID, TaskRecord taskRecord) {
        m_hostID = hostID;
        m_taskRecord = taskRecord;
        assert m_taskRecord.getTaskID().equals(getTaskID());
    }

    /**
     * Subclasses must implement this method, which is called to execute the task.
     */
    public abstract void execute();
    
    /**
     * Subclasses can call this to report progress on their task. Long-running tasks
     * should call this periodically to prevent warnings about the task potentially being
     * hung.
     * 
     * @param progressMessage   Progress report message. Persisted in the Tasks table and
     *                          visible via the "get tasks" command.
     */
    protected final void reportProgress(String progressMessage) {
        m_lastProgressTimestamp = System.currentTimeMillis();
        m_progressMessage = progressMessage;
        updateTaskProgress();
    }
    
    /**
     * Called by the TaskManagerService to begin the execution of the task.
     */
    @Override
    public final void run() {
        String taskID = m_taskRecord.getTaskID();
        m_logger.debug("Starting task '{}' in tenant '{}'", taskID, m_tenant);
        try {
            TaskManagerService.instance().registerTaskStarted(this);
            m_lastProgressTimestamp = System.currentTimeMillis();
            setTaskStart();
            execute();
            setTaskFinish();
        } catch (Throwable e) {
            m_logger.error("Task '" + taskID + "' failed", e);
            String stackTrace = Utils.getStackTrace(e);
            setTaskFailed(stackTrace);
        } finally {
            TaskManagerService.instance().registerTaskEnded(this);
        }
    }

    /**
     * Get the tenant that owns this task executor's application.
     * 
     * @return  {@link Tenant} for this task execution.
     */
    Tenant getTenant() {
        return m_tenant;
    }
    
    /**
     * Get the frequency at which this task is defined to execute.
     * 
     * @return  Task frequency as a {@link TaskFrequency} object.
     */
    public TaskFrequency getTaskFreq() {
        return m_taskFreq;
    }
    
    /**
     * Get this task's ID, which is a three part string such as:
     * <pre>
     *      foo/bar/data-aging
     * </pre>
     * 
     * @return  This task's ID.
     */
    public String getTaskID() {
        return m_appName + "/" + m_tableName + "/" + m_taskName;
    }
    
    /**
     * Same as {@link #getTaskID()}.
     */
    @Override
    public String toString() {
        return getTaskID();
    }

    //----- Private methods 
    
    // Update the job status record that shows this job has started.
    private void setTaskStart() {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_START_TIME, Long.toString(System.currentTimeMillis()));
        m_taskRecord.setProperty(TaskRecord.PROP_FINISH_TIME, null);
        m_taskRecord.setProperty(TaskRecord.PROP_PROGRESS, null);
        m_taskRecord.setProperty(TaskRecord.PROP_PROGRESS_TIME, null);
        m_taskRecord.setProperty(TaskRecord.PROP_FAIL_REASON, null);
        m_taskRecord.setStatus(TaskStatus.IN_PROGRESS);
        TaskManagerService.instance().updateTaskStatus(m_tenant, m_taskRecord, false);
    }
    
    // Update the job status record that shows this job has finished.
    private void updateTaskProgress() {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_PROGRESS, m_progressMessage);
        m_taskRecord.setProperty(TaskRecord.PROP_PROGRESS_TIME, Long.toString(m_lastProgressTimestamp));
        TaskManagerService.instance().updateTaskStatus(m_tenant, m_taskRecord, false);
    }
    
    // Update the job status record that shows this job has finished.
    private void setTaskFinish() {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_FINISH_TIME, Long.toString(System.currentTimeMillis()));
        m_taskRecord.setStatus(TaskStatus.COMPLETED);
        TaskManagerService.instance().updateTaskStatus(m_tenant, m_taskRecord, true);
    }
    
    // Update the job status record that shows this job has failed.
    private void setTaskFailed(String reason) {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_FINISH_TIME, Long.toString(System.currentTimeMillis()));
        m_taskRecord.setProperty(TaskRecord.PROP_FAIL_REASON, reason);
        m_taskRecord.setStatus(TaskStatus.FAILED);
        TaskManagerService.instance().updateTaskStatus(m_tenant, m_taskRecord, true);
    }
    
}   // class Task
