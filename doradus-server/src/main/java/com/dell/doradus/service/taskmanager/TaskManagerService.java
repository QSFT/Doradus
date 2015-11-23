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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.DBManagerService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.TaskRecord.TaskStatus;
import com.dell.doradus.service.tenant.TenantService;

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
    private static final int TASK_CHECK_MILLIS = 1000;
    
    // Threshold at which we consider our own task possibly hung:
    private static final int HUNG_TASK_THRESHOLD_MINS = 30;
    
    // Threshold at which we consider a remote task possibly abandoned; this value
    // must be greater than HUNG_TASK_THRESHOLD_MINS:
    private static final int DEAD_TASK_THRESHOLD_MINS = HUNG_TASK_THRESHOLD_MINS + 5;
    
    // Singleton object:
    private static final TaskManagerService INSTANCE = new TaskManagerService();
    
    // Members:
    private Thread m_taskManager;
    private boolean m_bShutdown;
    private String  m_localHost;
    private String  m_hostClaimID = UUID.randomUUID().toString();
    
    // Task execution management:
    private final ExecutorService m_executor = Executors.newFixedThreadPool(MAX_TASKS);
    private final Map<String, Task> m_activeTasks = new HashMap<>();
    private final Object m_executeLock = new Object();
    
    // REST commands registered:
    private static final List<Class<? extends RESTCallback>> CMD_CLASSES = Arrays.asList(
        ListTasksCmd.class
    );
    
    // Singleton creation only
    private TaskManagerService() {}
    
    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static TaskManagerService instance() {
        return INSTANCE;
    }   // instance
    
    //----- Service methods
    
    @Override
    protected void initService() {
        RESTService.instance().registerCommands(CMD_CLASSES);
    }

    @Override
    protected void startService() {
        DBManagerService.instance().waitForFullService();
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
            if (m_taskManager != null) {
                m_taskManager.interrupt();
                try {
                    m_taskManager.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }   // stopService

    //----- Public methods
    
    /**
     * Execute the given task and return the task execution status. If the task is already
     * running by this or another task manager, this method blocks until the task has
     * finished. (TODO: should we put a timeout on this?) Once the task is not running, it
     * then executed, even if we had to wait for a previous execution. In a multi-node
     * cluster, the task may be executed by another node if just happened to be scheduled
     * for execution, but in most cases, this task manager will execute the task.
     * Regardless of whether this or a remote task manager executes the task, this method
     * waits for that execution to complete and returns the final task status.
     *   
     * @param appDef    {@link ApplicationDefinition} that defines the task's context
     *                  including its tenant.
     * @param task      Application-specific {@link Task} to execute.
     * @return          Final {@link TaskStatus} of the task's execution.
     */
    public TaskStatus executeTask(ApplicationDefinition appDef, Task task) {
        checkServiceState();
        Tenant tenant = Tenant.getTenant(appDef);
        m_logger.debug("Checking that task {} in tenant {} is not running", task.getTaskID(), tenant);
        TaskRecord taskRecord = null;
        synchronized (m_executeLock) {
            taskRecord = waitForTaskStatus(tenant, task, s -> s != TaskStatus.IN_PROGRESS);
            taskRecord.setStatus(TaskStatus.NEVER_EXECUTED);
            updateTaskStatus(tenant, taskRecord, false);
            attemptToExecuteTask(appDef, task, taskRecord);
        }
        m_logger.debug("Checking that task {} in tenant {} has completed", tenant, task.getTaskID());
        taskRecord = waitForTaskStatus(tenant, task, s -> TaskStatus.isCompleted(s));
        return taskRecord.getStatus();
    }

    /**
     * Delet all task status rows, if any, related to the given application from the Tasks
     * table. NOTE: This method can be called even if the TaskManagerService has not been
     * initialized so that we can always cleanup deleted applications.
     * 
     * @param appDef    {@link ApplicationDefinition} of application being deleted.
     */
    public void deleteApplicationTasks(ApplicationDefinition appDef) {
        String prefixName = appDef.getAppName() + "/";
        String claimPrefixName = "_claim/" + prefixName;
        Tenant tenant = Tenant.getTenant(appDef);
        DBTransaction dbTran = DBService.instance(tenant).startTransaction();
        for(DRow row: DBService.instance(tenant).getAllRows(TaskManagerService.TASKS_STORE_NAME)) {
            if (row.getKey().startsWith(prefixName) ||
                row.getKey().startsWith(claimPrefixName)) {
                dbTran.deleteRow(TASKS_STORE_NAME, row.getKey());
            }
        }
        if (dbTran.getMutationsCount() > 0) {
            m_logger.info("Deleting {} task status rows for application {}",
                          dbTran.getMutationsCount(), appDef.getAppName());
            DBService.instance(tenant).commit(dbTran);
        }
    }
    
    //----- Package-private methods
    
    /**
     * Called by a Task when it's thread starts. This lets the task manager know
     * which tasks are its own.
     * 
     * @param task  {@link Task} that is executing task.
     */
    void registerTaskStarted(Task task) {
        synchronized (m_activeTasks) {
            String mapKey = createMapKey(task.getTenant(), task.getTaskID());
            if (m_activeTasks.put(mapKey, task) != null) {
                m_logger.warn("Task {} registered as started but was already running", mapKey);
            }
        }
    }
    
    /**
     * Called by a Task when it's thread finishes. This lets the task manager know
     * that another slot has opened-up.
     * 
     * @param task   {@link Task} that just finished.
     */
    void registerTaskEnded(Task task) {
        synchronized (m_activeTasks) {
            String mapKey = createMapKey(task.getTenant(), task.getTaskID());
            if (m_activeTasks.remove(mapKey) == null) {
                m_logger.warn("Task {} registered as ended but was not running", mapKey);
            }
        }
    }
    
    /**
     * Return all {@link TaskRecord}s stored in the Tasks table for the given tenant. This
     * provides an account of all known tasks, past and present.
     * 
     * @param tenant    {@link Tenant} to query.
     * @return          Collection of TaskRecords representing all known task statuses.
     */
    Collection<TaskRecord> getTaskRecords(Tenant tenant) {
        checkServiceState();
        List<TaskRecord> taskRecords = new ArrayList<>();
        for(DRow row: DBService.instance(tenant).getAllRows(TaskManagerService.TASKS_STORE_NAME)) {
            String taskID = row.getKey();
            if (taskID.startsWith("_claim/")) {
                continue;
            }
            Iterator<DColumn> colIter = row.getAllColumns(1024).iterator();
            TaskRecord taskRecord = buildTaskRecord(taskID, colIter);
            taskRecords.add(taskRecord);
        }
        return taskRecords;
    }

    /**
     * Add or update a task status record and optionally delete the task's claim record at
     * the same time.
     * 
     * @param tenant                {@link Tenant} that owns the task's application.
     * @param taskRecord            {@link TaskRecord} containing task properties to be
     *                              written to the database. A null/empty property value
     *                              causes the corresponding column to be deleted.
     * @param bDeleteClaimRecord    True to delete the task's claim record in the same
     *                              transaction.
     */
    void updateTaskStatus(Tenant tenant, TaskRecord taskRecord, boolean bDeleteClaimRecord) {
        String taskID = taskRecord.getTaskID();
        DBTransaction dbTran = DBService.instance(tenant).startTransaction();
        Map<String, String> propMap = taskRecord.getProperties();
        for (String name : propMap.keySet()) {
            String value = propMap.get(name);
            if (Utils.isEmpty(value)) {
                dbTran.deleteColumn(TaskManagerService.TASKS_STORE_NAME, taskID, name);
            } else {
                dbTran.addColumn(TaskManagerService.TASKS_STORE_NAME, taskID, name, value);
            }
        }
        if (bDeleteClaimRecord) {
            dbTran.deleteRow(TaskManagerService.TASKS_STORE_NAME, "_claim/" + taskID);
        }
        DBService.instance(tenant).commit(dbTran);
    }
    
    //----- Private methods

    // Wait for the given task to achieve the given TaskStatus predicate, then return the
    // latest status record. If it has never run, a never-run task record is stored.
    private TaskRecord waitForTaskStatus(Tenant tenant, Task task, Predicate<TaskStatus> pred) {
        TaskRecord taskRecord = null;
        while (true) {
            Iterator<DColumn> colIter =
                DBService.instance(tenant).getAllColumns(TaskManagerService.TASKS_STORE_NAME, task.getTaskID()).iterator();
            if (!colIter.hasNext()) {
                taskRecord = storeTaskRecord(tenant, task);
            } else {
                taskRecord = buildTaskRecord(task.getTaskID(), colIter);
            }
            if (pred.test(taskRecord.getStatus())) {
                break;
            }
            try { Thread.sleep(TASK_CHECK_MILLIS); } catch (InterruptedException e) { }
        };
        return taskRecord;
    }

    // Thread entrypoint when the TaskManagerService starts. Wake-up periodically and look for
    // tasks we can run. Shutdown when told to do so.
    private void manageTasks() {
        setHostAddress();
        while (!m_bShutdown) {
            checkAllTasks();
            checkForDeadTasks();
            try {
                Thread.sleep(SLEEP_TIME_MILLIS);
            } catch (InterruptedException e) {
            }
        }
        m_executor.shutdown();
    }   // manageTasks
    
    // Check all tenants for tasks that need execution.
    private void checkAllTasks() {
        for (Tenant tenant: TenantService.instance().getTenants()) {
            checkTenantTasks(tenant);
            if (m_bShutdown) {
                break;
            }
        }
    }   // checkAllTasks
    
    // Check the given tenant for tasks that need execution.
    private void checkTenantTasks(Tenant tenant) {
        m_logger.debug("Checking tenant '{}' for needy tasks", tenant);
        try {
            for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications(tenant)) {
                for (Task task : getAppTasks(appDef)) {
                    checkTaskForExecution(appDef, task);
                }
            }
        } catch (Throwable e) {
            m_logger.warn("Could not check tasks for tenant '{}': {}", tenant.getName(), e);
        }
    }   // checkTenantTasks

    // Check the given task to see if we should and can execute it.
    private void checkTaskForExecution(ApplicationDefinition appDef, Task task) {
        Tenant tenant = Tenant.getTenant(appDef);
        m_logger.debug("Checking task '{}' in tenant '{}'", task.getTaskID(), tenant);
        synchronized (m_executeLock) {
            Iterator<DColumn> colIter =
                DBService.instance(tenant).getAllColumns(TaskManagerService.TASKS_STORE_NAME, task.getTaskID()).iterator();
            TaskRecord taskRecord = null;
            if (!colIter.hasNext()) {
                taskRecord = storeTaskRecord(tenant, task);
            } else {
                taskRecord = buildTaskRecord(task.getTaskID(), colIter);
            }
            if (taskShouldExecute(task, taskRecord) && canHandleMoreTasks()) {
                attemptToExecuteTask(appDef, task, taskRecord);
            }
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
        
        Calendar startTime = taskRecord.getTime(TaskRecord.PROP_START_TIME);
        long startTimeMillis = startTime == null ? 0 : startTime.getTimeInMillis();
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
        synchronized (m_activeTasks) {
            return m_activeTasks.size() < MAX_TASKS;
        }
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
        } else {
        	m_logger.info("Will not start task: it was claimed by another service");
        }
    }

    // Execute the given task by handing it to the ExecutorService.
    private void startTask(ApplicationDefinition appDef, Task task, TaskRecord taskRecord) {
        try {
            task.setParams(m_localHost, taskRecord);
            m_executor.execute(task);
        } catch (Exception e) {
            m_logger.error("Failed to start task '" + task.getTaskID() + "'", e);
        }
    }   // startTask

    // Indicate if we won the claim to run the given task.
    private boolean taskClaimedByUs(Tenant tenant, String claimID) {
        waitForClaim();
        Iterator<DColumn> colIter =
            DBService.instance(tenant).getAllColumns(TaskManagerService.TASKS_STORE_NAME, claimID).iterator();
        if (colIter == null) {
            m_logger.warn("Claim record disappeared: {}", claimID);
            return false;
        }
        String claimingHost = m_hostClaimID;
        long earliestClaim = Long.MAX_VALUE;
        while (colIter.hasNext()) {
            DColumn col = colIter.next();
            try {
                long claimStamp = Long.parseLong(col.getValue());
                // otarakanov: sometimes, the task writes a claim but does not start. The claim remains the lowest
                // and makes future tries to write new claims but not start.  
                // we disregard claims older that ten minutes.
                long secondsSinceClaim = (System.currentTimeMillis() - claimStamp) / 1000;
                if(secondsSinceClaim > 600) continue;
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
        return claimingHost.equals(m_hostClaimID) && !m_bShutdown;
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
        DBTransaction dbTran = DBService.instance(tenant).startTransaction();
        dbTran.addColumn(TaskManagerService.TASKS_STORE_NAME, claimID, m_hostClaimID, claimStamp);
        DBService.instance(tenant).commit(dbTran);
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
        DBTransaction dbTran = DBService.instance(tenant).startTransaction();
        TaskRecord taskRecord = new TaskRecord(task.getTaskID());
        Map<String, String> propMap = taskRecord.getProperties();
        assert propMap.size() > 0 : "Need at least one property to store a row!";
        for (String propName : propMap.keySet()) {
            dbTran.addColumn(TaskManagerService.TASKS_STORE_NAME, task.getTaskID(), propName, propMap.get(propName));
        }
        DBService.instance(tenant).commit(dbTran);
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

    // Look for tasks that have not reported or finished for too long.
    private void checkForDeadTasks() {
        for (Tenant tenant: TenantService.instance().getTenants()) {
            checkForDeadTenantTasks(tenant);
            if (m_bShutdown) {
                break;
            }
        }
    }
    
    // Look for hung/abandoned tasks in the given tenant.
    private void checkForDeadTenantTasks(Tenant tenant) {
        m_logger.debug("Checking tenant {} for abandoned tasks", tenant);
        try {
            for(DRow row: DBService.instance(tenant).getAllRows(TaskManagerService.TASKS_STORE_NAME)) {
                TaskRecord taskRecord = buildTaskRecord(row.getKey(), row.getAllColumns(1024).iterator());
                if (taskRecord.getStatus() == TaskStatus.IN_PROGRESS) {
                    checkForDeadTask(tenant, taskRecord);
                }
            }
        } catch (Throwable e) {
            m_logger.warn("Unable to check tenant '{}' for dead tasks: {}", tenant.getName(), e.toString());
        }
    }

    // See if the given in-progress task may be hung or abandoned.
    private void checkForDeadTask(Tenant tenant, TaskRecord taskRecord) {
        Calendar lastReport = taskRecord.getTime(TaskRecord.PROP_PROGRESS_TIME);
        if (lastReport == null) {
            lastReport = taskRecord.getTime(TaskRecord.PROP_START_TIME);
            if (lastReport == null) {
                return; // corrupt/incomplete task record
            }
        }
        long minsSinceLastActivity = (System.currentTimeMillis() - lastReport.getTimeInMillis()) / (1000 * 60);
        if (isOurActiveTask(tenant, taskRecord.getTaskID())) {
            checkForHungTask(tenant, taskRecord, minsSinceLastActivity);
        } else {
            checkForAbandonedTask(tenant, taskRecord, minsSinceLastActivity);
        }
    }

    // If the given local task has not reported progress for HUNG_TASK_THRESHOLD_MINS, log
    // a warning that the task may be hung. But let it continue running and prevent remote
    // task managers from marking it as dead by bumping its progress stamp. This blocks a
    // potentially harmful second task execution.
    private void checkForHungTask(Tenant tenant, TaskRecord taskRecord, long minsSinceLastActivity) {
        if (minsSinceLastActivity > HUNG_TASK_THRESHOLD_MINS) {
            String taskIdentity = createMapKey(tenant, taskRecord.getTaskID());
            String reason = "No progress reported in " + minsSinceLastActivity + " minutes";
            m_logger.warn("Local task {} has not reported progress in {} minutes; " +
                          "restart the server if this continues for too long",
                          taskIdentity, minsSinceLastActivity);
            taskRecord.setProperty(TaskRecord.PROP_PROGRESS_TIME, Long.toString(System.currentTimeMillis()));
            taskRecord.setProperty(TaskRecord.PROP_PROGRESS, reason);
            updateTaskStatus(tenant, taskRecord, false);
        }
    }

    // If the given remote task has not reported progress for DEAD_TASK_THRESHOLD_MINS,
    // assume the task's containing process has died and mark it as failed. This allows it
    // to restart upon the next check-tasks cycle.
    private void checkForAbandonedTask(Tenant tenant, TaskRecord taskRecord, long minsSinceLastActivity) {
        if (minsSinceLastActivity > DEAD_TASK_THRESHOLD_MINS) {
            String taskIdentity = createMapKey(tenant, taskRecord.getTaskID());
            String reason = "No progress reported in " + minsSinceLastActivity + " minutes";
            m_logger.error("Remote task {} has not reported progress in {} minutes; marking as failed",
                           taskIdentity, minsSinceLastActivity);
            taskRecord.setProperty(TaskRecord.PROP_FINISH_TIME, Long.toString(System.currentTimeMillis()));
            taskRecord.setProperty(TaskRecord.PROP_FAIL_REASON, reason);
            taskRecord.setStatus(TaskStatus.FAILED);
            updateTaskStatus(tenant, taskRecord, true);
        }
    }

    // Construct the map key used to track task executions we own.
    private static String createMapKey(Tenant tenant, String taskID) {
        return tenant.getName() + "/" + taskID;
    }

    // Return true if we are currently executing the given task.
    private boolean isOurActiveTask(Tenant tenant, String taskID) {
        synchronized (m_activeTasks) {
            return m_activeTasks.containsKey(createMapKey(tenant, taskID));
        }
    }

    // Get the host address to use for task claiming.
    private void setHostAddress() {
        try {
            m_localHost = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            m_localHost = "0.0.0.0";
        }
    }   // setHostAddress

}   // class TaskManagerService
