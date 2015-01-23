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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.taskmanager.TaskRecord.TaskStatus;

/**
 * Abstract class for task execution objects. Each {@link Task} identifies a subclass of
 * this class that must be used to perform the corresponding task. 
 */
public abstract class TaskExecutor implements Runnable {
    // Protected logger available to concrete services:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    // Members:
    protected String                  m_hostID;
    protected Tenant                  m_tenant;
    protected ApplicationDefinition   m_appDef;
    protected TaskRecord              m_taskRecord;
    
    /**
     * A zero-argument constructor is required so that objects can be dynamically created.
     * When a TaskExecutor is created, {@link #setParams(String, ApplicationDefinition, TaskRecord)}
     * is immediately called to set the job's parameters. 
     */
    public TaskExecutor() {}
    
    /**
     * Set the execution parameters for this job. This method is called by the
     * {@link TaskManagerService} immediately after constructing the job object.
     *  
     * @param hostID        Identifier of the node that is executing the task, usually the
     *                      IP address.
     * @param appDef        {@link ApplicationDefinition} of application for which the job
     *                      is executing.
     * @param taskRecord    Object used to track the status of the task execution.
     */
    void setParams(String hostID, ApplicationDefinition appDef, TaskRecord taskRecord) {
        m_hostID = hostID;
        m_tenant = Tenant.getTenant(appDef);
        m_appDef = appDef;
        m_taskRecord = taskRecord;
    }

    /**
     * Subclasses must implement this method, which is called to execute the task. This
     * method is only called after {@link #setParams(String, ApplicationDefinition, TaskRecord)}
     * is called.
     */
    public abstract void execute();
    
    /**
     * Called by the TaskManagerService to begin the execution of the task.
     */
    @Override
    public final void run() {
        String taskID = m_taskRecord.getTaskID();
        m_logger.debug("Starting task '{}' in tenant '{}'", taskID, m_tenant);
        try {
            TaskManagerService.instance().incrementActiveTasks();
            setTaskStart();
            execute();
            setTaskFinish();
        } catch (Throwable e) {
            m_logger.error("Task '{}" + taskID + "' failed", e);
            setTaskFailed();
        } finally {
            TaskManagerService.instance().decrementActiveTasks();
        }
    }

    /**
     * Get the application to which this task applies.
     * 
     * @return  {@link ApplicationDefinition} of application for this task.
     */
    public ApplicationDefinition getAppDef() {
        return m_appDef;
    }
    
    //----- Private methods 
    
    // Update the job status record that shows this job has started.
    private void setTaskStart() {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_START_TIME, Long.toString(System.currentTimeMillis()));
        m_taskRecord.setStatus(TaskStatus.IN_PROGRESS);
        updateTaskStatus(false);
    }
    
    // Update the job status record that shows this job has finished.
    private void setTaskFinish() {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_FINISH_TIME, Long.toString(System.currentTimeMillis()));
        m_taskRecord.setStatus(TaskStatus.COMPLETED);
        updateTaskStatus(true);
    }
    
    // Update the job status record that shows this job has failed.
    private void setTaskFailed() {
        m_taskRecord.setProperty(TaskRecord.PROP_EXECUTOR, m_hostID);
        m_taskRecord.setProperty(TaskRecord.PROP_FINISH_TIME, Long.toString(System.currentTimeMillis()));
        m_taskRecord.setStatus(TaskStatus.FAILED);
        updateTaskStatus(true);
    }
    
    // Write the current job status record into the database and optionally delete the
    // job's claim record at the same time.
    private void updateTaskStatus(boolean bDeleteClaimRecord) {
        String taskID = m_taskRecord.getTaskID();
        DBTransaction dbTran = DBService.instance().startTransaction(m_tenant);
        Map<String, String> propMap = m_taskRecord.getProperties();
        for (String name : propMap.keySet()) {
            dbTran.addColumn(TaskManagerService.TASKS_STORE_NAME, taskID, name, propMap.get(name));
        }
        if (bDeleteClaimRecord) {
            dbTran.deleteRow(TaskManagerService.TASKS_STORE_NAME, "_claim/" + taskID);
        }
        DBService.instance().commit(dbTran);
    }   // updateJobStatus
    
}   // class TaskExecutor
