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

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.TaskRecord.TaskStatus;

/**
 * Provides task execution service for Doradus. If this service is enabled, it looks for
 * tasks across all tenants and applications and executes them on schedule. Multiple
 * Doradus nodes may be executing in a cluster, each of which may have a TaskManager
 * service enabled. To prevent duplicate/overlapping executions of the same task, a simple
 * "claim" algorithm is used to ensure only one Doradus instance executes any given task.
 */
public class TaskManagerService extends Service {
    // Tasks ColumnFamily name:
    public static final String TASKS_STORE_NAME = "Tasks";
    
    // Hard-coded constants:
    private static final int SLEEP_TIME_MILLIS = 60000;
    private static final int CLAIM_WAIT_MILLIS = 1000;
    private static final int MAX_TASKS = 2;
    
    // Singleton object:
    private static final TaskManagerService INSTANCE = new TaskManagerService();
    
    // Members:
    private Thread m_taskManager;
    private boolean m_bShutdown;
    private String  m_localHost;
    
    // Task execution management:
    private final ExecutorService m_executor = Executors.newFixedThreadPool(MAX_TASKS);
    private final AtomicInteger   m_currentTasks = new AtomicInteger();
    
    // REST commands registered:
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[] {
        new RESTCommand("GET /_tasks com.dell.doradus.service.taskmanager.ListTasksCmd"),
    });
    
    //----- Public methods
    
    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static TaskManagerService instance() {
        return INSTANCE;
    }   // instance
    
    @Override
    protected void initService() {
        RESTService.instance().registerRESTCommands(REST_RULES);
    }

    @Override
    protected void startService() {
        DBService.instance().waitForFullService();
        m_taskManager = new Thread("Task Manager") {
            @Override
            public void run() {
                manageTasks();
            }
        };
        m_taskManager.start();
    }

    @Override
    protected void stopService() {
        if (getState().isRunning()) {
            m_bShutdown = true;
            m_taskManager.interrupt();
            try {
                m_taskManager.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }   // stopService

    /**
     * Increment the number of active tasks counted. Each call to this method must be
     * paired with a reciprical call to {@link #decrementActiveTasks()}. 
     */
    public void incrementActiveTasks() {
        m_currentTasks.incrementAndGet();
    }

    /**
     * Decrement the number of active tasks counted. This method must be called after
     * {@link #incrementActiveTasks()} is called when a task finishes, good or bad.
     */
    public void decrementActiveTasks() {
        m_currentTasks.decrementAndGet();
    }
    
    /**
     * Return all {@link TaskRecord}s stored in the Tasks table for the given tenant. This
     * provides an account of all known tasks, past and present.
     * 
     * @param tenant    {@link Tenant} to query.
     * @return          Collection of TaskRecords representing all known task statuses.
     */
    public Collection<TaskRecord> getTaskRecords(Tenant tenant) {
        checkServiceState();
        Iterator<DRow> rowIter =
            DBService.instance().getAllRowsAllColumns(tenant, TaskManagerService.TASKS_STORE_NAME);
        List<TaskRecord> taskRecords = new ArrayList<>();
        while (rowIter.hasNext()) {
            DRow row = rowIter.next();
            String taskID = row.getKey();
            if (taskID.startsWith("_claim/")) {
                continue;
            }
            Iterator<DColumn> colIter = row.getColumns();
            TaskRecord taskRecord = buildTaskRecord(taskID, colIter);
            taskRecords.add(taskRecord);
        }
        return taskRecords;
    }

    //----- Private methods

    // Thread entrypoint when the TaskManagerService starts. Wake-up periodically and look for
    // tasks we can run. Shutdown when told to do so.
    private void manageTasks() {
        setHostAddress();
        while (!m_bShutdown) {
            checkAllTasks();
            try {
                Thread.sleep(SLEEP_TIME_MILLIS);
            } catch (InterruptedException e) {
            }
        }
        m_executor.shutdown();
    }   // manageTasks
    
    // Check all tenants for tasks that need execution.
    private void checkAllTasks() {
        for (Tenant tenant: DBService.instance().getTenants()) {
            checkTenantTasks(tenant);
            if (m_bShutdown) {
                break;
            }
        }
    }   // checkAllTasks
    
    // Check the given tenant for tasks that need execution.
    private void checkTenantTasks(Tenant tenant) {
        m_logger.debug("Checking tenant '{}' for needy tasks", tenant);
        for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications(tenant)) {
            for (Task task : getAppTasks(appDef)) {
                checkTaskForExecution(appDef, task);
            }
        }
    }   // checkTenantTasks

    // Check the given task to see if we should and can execute it.
    private void checkTaskForExecution(ApplicationDefinition appDef, Task task) {
        Tenant tenant = Tenant.getTenant(appDef);
        m_logger.debug("Checking task '{}' in tenant '{}'", task.getTaskID(), tenant);
        Iterator<DColumn> colIter =
            DBService.instance().getAllColumns(tenant, TaskManagerService.TASKS_STORE_NAME, task.getTaskID());
        TaskRecord taskRecord = null;
        if (colIter == null) {
            taskRecord = storeTaskRecord(tenant, task);
        } else {
            taskRecord = buildTaskRecord(task.getTaskID(), colIter);
        }
        if (taskShouldExecute(task, taskRecord) && canHandleMoreTasks()) {
            attemptToExecuteTask(appDef, task, taskRecord);
        }
    }   // checkTaskForExecution

    // Indicate if the given task should be executed. This is always true for a task that
    // has never executed. It is also true if (1) the task is not already being executed,
    // and (2) enough time has passed since its last execution that it's time to run.
    private boolean taskShouldExecute(Task task, TaskRecord taskRecord) {
        String taskID = taskRecord.getTaskID();
        if (taskRecord.getStatus() == TaskStatus.NEVER_EXECUTED) {
            m_logger.debug("Task '{}' has never executed", taskID);
            return true;
        }
        if (taskRecord.getStatus() == TaskStatus.IN_PROGRESS) {
            m_logger.debug("Task '{}' is already being executed", taskID);
            return false;
        }
        
        long startTimeMillis = taskRecord.getStartTime().getTimeInMillis();
        long taskPeriodMillis = task.getTaskFreq().getValueInMinutes() * 60 * 1000;
        long nowMillis = System.currentTimeMillis();
        boolean bShouldStart = startTimeMillis + taskPeriodMillis <= nowMillis;
        m_logger.debug("Considering task {}: Last started at {}; periodicity in millis: {}; current time: {}; next start: {}; should start: {}",
                      new Object[]{task.getTaskID(),
                                   Utils.formatDateUTC(startTimeMillis, Calendar.MILLISECOND),
                                   taskPeriodMillis,
                                   Utils.formatDateUTC(nowMillis, Calendar.MILLISECOND),
                                   Utils.formatDateUTC(startTimeMillis + taskPeriodMillis, Calendar.MILLISECOND),
                                   bShouldStart});
        return bShouldStart;
    }   // taskShouldExecute

    // Indicate if we have room to execute another task.
    private boolean canHandleMoreTasks() {
        return m_currentTasks.get() < MAX_TASKS;
    }
    
    // Attempt to start the given task by creating claim and see if we win it.
    private void attemptToExecuteTask(ApplicationDefinition appDef, Task task, TaskRecord taskRecord) {
        Tenant tenant = Tenant.getTenant(appDef);
        String taskID = taskRecord.getTaskID();
        String claimID = "_claim/" + taskID;
        long claimStamp = System.currentTimeMillis();
        writeTaskClaim(tenant, claimID, claimStamp);
        if (taskClaimedByUs(tenant, claimID)) {
            startTask(appDef, task, taskRecord);
        }
    }

    // Execute the given task by creating a TaskExecutor for it and handing to the
    // ExecutorService.
    private void startTask(ApplicationDefinition appDef, Task task, TaskRecord taskRecord) {
        try {
            Class<? extends TaskExecutor> jobClass = task.getExecutorClass();
            Constructor<?> noArgConstructor = jobClass.getConstructor((Class<?>[])null);
            TaskExecutor executor = (TaskExecutor) noArgConstructor.newInstance((Object[])null);
            executor.setParams(m_localHost, appDef, taskRecord);
            m_executor.execute(executor);
        } catch (Exception e) {
            m_logger.error("Failed to start task '" + task.getTaskID() + "'", e);
        }
    }   // startTask

    // Indicate if we won the claim to run the given task.
    private boolean taskClaimedByUs(Tenant tenant, String claimID) {
        waitForClaim();
        Iterator<DColumn> colIter =
            DBService.instance().getAllColumns(tenant, TaskManagerService.TASKS_STORE_NAME, claimID);
        if (colIter == null) {
            m_logger.warn("Claim record disappeared: {}", claimID);
            return false;
        }
        String claimingHost = m_localHost;
        long earliestClaim = Long.MAX_VALUE;
        while (colIter.hasNext()) {
            DColumn col = colIter.next();
            try {
                long claimStamp = Long.parseLong(col.getValue());
                String claimHost = col.getName();
                if (claimStamp < earliestClaim) {
                    claimingHost = claimHost;
                    earliestClaim = claimStamp;
                } else if (claimStamp == earliestClaim) {
                    // Two nodes chose the same claim stamp. Lower node name wins.
                    if (claimHost.compareTo(claimingHost) < 0) {
                        claimingHost = claimHost;
                    }
                }
            } catch (NumberFormatException e) {
                // Ignore this column
            }
        }
        return claimingHost.equals(m_localHost) && !m_bShutdown;
    }   // taskClaimedByUs

    // Sleep the configured amount of time for other hosts to stake their claim.
    private void waitForClaim() {
        try {
            Thread.sleep(CLAIM_WAIT_MILLIS);
        } catch (InterruptedException e) {
        }
    }   // waitForClaim
    
    // Write a claim record to the Tasks table.
    private void writeTaskClaim(Tenant tenant, String claimID, long claimStamp) {
        DBTransaction dbTran = DBService.instance().startTransaction(tenant);
        dbTran.addColumn(TaskManagerService.TASKS_STORE_NAME, claimID, m_localHost, claimStamp);
        DBService.instance().commit(dbTran);
    }   // writeTaskClaim
    
    // Create a TaskRecord from a task status row read from the Tasks table.
    private TaskRecord buildTaskRecord(String taskID, Iterator<DColumn> colIter) {
        TaskRecord taskRecord = new TaskRecord(taskID);
        while (colIter.hasNext()) {
            DColumn col = colIter.next();
            taskRecord.setProperty(col.getName(), col.getValue());
        }
        return taskRecord;
    }   // buildTaskRecord

    // Create a TaskRecord for the given task and write it to the Tasks table.
    private TaskRecord storeTaskRecord(Tenant tenant, Task task) {
        DBTransaction dbTran = DBService.instance().startTransaction(tenant);
        TaskRecord taskRecord = new TaskRecord(task.getTaskID());
        Map<String, String> propMap = taskRecord.getProperties();
        assert propMap.size() > 0 : "Need at least one property to store a row!";
        for (String propName : propMap.keySet()) {
            dbTran.addColumn(TaskManagerService.TASKS_STORE_NAME, task.getTaskID(), propName, propMap.get(propName));
        }
        DBService.instance().commit(dbTran);
        return taskRecord;
    }   // storeTaskRecord

    // Ask the storage manager for the given application for its required tasks.
    private List<Task> getAppTasks(ApplicationDefinition appDef) {
        List<Task> appTasks = new ArrayList<>();
        try {
            StorageService service = SchemaService.instance().getStorageService(appDef);
            Collection<Task> appTaskColl = service.getAppTasks(appDef);
            if (appTaskColl != null) {
                appTasks.addAll(service.getAppTasks(appDef));
            }
        } catch (IllegalArgumentException e) {
            // StorageService has not been initialized; no tasks for this application.
        }
        return appTasks;
    }   // getAppTasks

    // Get the host address to use for task claiming.
    private void setHostAddress() {
        try {
            m_localHost = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            m_localHost = "0.0.0.0";
        }
    }   // setHostAddress

}   // class TaskManagerService
