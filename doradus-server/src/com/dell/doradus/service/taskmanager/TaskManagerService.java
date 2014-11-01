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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskExecutor;
import it.sauronsoftware.cron4j.TaskTable;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ScheduleDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.management.TaskRunState;
import com.dell.doradus.management.TaskSettings;
import com.dell.doradus.management.TaskStatus;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.tasks.DoradusTask;
import com.dell.doradus.tasks.ITaskManager;

/**
 * Background tasks service.
 * 
 * The service starts an instance of tasks scheduler to set a series of
 * timers that would start background tasks according to their time schedule.
 * Every Doradus node is allowed to perform only a limited number of tasks,
 * so in multi-node environment all the Doradus nodes would compete for
 * tasks to run.
 * 
 * The service implements an interface ITaskManager to serve as a JMX service
 * too. Doradus Console application uses this service to control background
 * tasks launching, interrupting, suspending, and so on.
 */
public class TaskManagerService extends Service implements ITaskManager {
	// Default number of tasks that can be started concurrently on the node
	public static final int MAX_NODE_TASKS = 2;
	// Delay (in seconds) before a task will be actually started.
	// Actually several more seconds will be wasted to cover possible errors in
	// nodes time synchronization.
	public static final int TASK_EXEC_DELAY = 5;
	// Maximal number of tasks restarts in case of tasks failures
	public static final int TASK_RESTARTS = 2;
	// Time interval (in seconds) between consequent checks whether some task
	// should be interrupted. 
	public static final int CHECK_INTERRUPTING_INTERVAL = 10;
	
	// Task scheduler
	Scheduler m_scheduler = new Scheduler();
	
	// Host Inet address
	private String m_localHost;
    
    // Singleton object:
    private static final TaskManagerService INSTANCE = new TaskManagerService();
    
    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static TaskManagerService instance() {
        return INSTANCE;
    }   // instance
    
    /**
     * Checks if the service is initialized and started
     * @return	Initializing flag
     */
    public boolean isInitialized() { return getState().isRunning(); }
    
    public String getLocalHostAddress() { return m_localHost; }
    
    ///// Service methods
    
    /**
     * Makes initial settings to task scheduler and registers REST commands
     */
    @Override
    public void initService() {
    	// Scheduler settings
        //m_scheduler.setTimeZone(TimeZone.getTimeZone("UTC"));
        m_scheduler.addTaskCollector(new TaskCollector() {
			@Override
			public TaskTable getTasks() {
				return TaskDBUtils.getTasksSchedule();
			}
        	
        });
        m_scheduler.addSchedulerListener(new DoradusSchedulerListener());
        
        // REST commands registering
        List<RESTCommand> restCommands = Arrays.asList(new RESTCommand[] {
        		new RESTCommand("GET    /_tasks                                 com.dell.doradus.service.taskmanager.ListTasksCmd"),
        		new RESTCommand("GET    /_tasks/{application}                   com.dell.doradus.service.taskmanager.ListTasksCmd"),
        		new RESTCommand("GET    /_tasks/{application}/{table}           com.dell.doradus.service.taskmanager.ListTasksCmd"),
        		new RESTCommand("GET    /_tasks/{application}/{table}/{task}    com.dell.doradus.service.taskmanager.ListTasksCmd"),

        		new RESTCommand("PUT    /_tasks?{command}                                 com.dell.doradus.service.taskmanager.TaskControlCmd"),
        		new RESTCommand("PUT    /_tasks/{application}?{command}                   com.dell.doradus.service.taskmanager.TaskControlCmd"),
        		new RESTCommand("PUT    /_tasks/{application}/{table}?{command}           com.dell.doradus.service.taskmanager.TaskControlCmd"),
        		new RESTCommand("PUT    /_tasks/{application}/{table}/{task}?{command}    com.dell.doradus.service.taskmanager.TaskControlCmd"),
        		new RESTCommand("PUT    /_tasks/{application}/{table}/{task}/{param}?{command}    com.dell.doradus.service.taskmanager.TaskControlCmd"),
        });
        RESTService.instance().registerRESTCommands(restCommands);
    }   // initService

    @Override
    public void startService() {
        DBService.instance().waitForFullService();
		try {
			m_localHost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			m_localHost = "0.0.0.0";
		}
		TaskDBUtils.checkUnknownTasks();
        TaskDBUtils.checkHangedTasks();
        m_scheduler.start();
        startCheckTaskInterruptions();
    }   // startService

    @Override
    public void stopService() {
        m_logger.info("Stopping");
        if (m_scheduler.isStarted()) {
            m_scheduler.stop();
        }
    }   // stopService
    
    /**
     * Number of currently executing background tasks on this node.
     * @return
     */
    public int runningTasksNumber() {
    	return m_scheduler.getExecutingTasks().length;
    }	// runningTasksNumber

	@Override
	public Set<String> getAppNames() throws IOException {
		Set<String> allNames = new HashSet<String>();
		for (ApplicationDefinition app : SchemaService.instance().getAllApplications()) {
			allNames.add(app.getAppName());
		}
		return allNames;
	}

	@Override
	public TaskSettings getGlobalDefaultSettings() {
		TaskSettings settings = new TaskSettings();
		if (ServerConfig.getInstance().default_schedule != null) {
			settings.setSchedule(ServerConfig.getInstance().default_schedule);
		}
		return  settings;
	}

	@Override
	public Map<String, TaskSettings> getAppSettings(String appName)
			throws IOException {
		ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
		Map<String, TaskSettings> mapTasks = new HashMap<String, TaskSettings>();
		
		// Extract the info about scheduled tasks
		Map<String, ScheduleDefinition> scheduleTasks = appDef.getSchedules();
		for(String key : scheduleTasks.keySet()) {
			ScheduleDefinition definition = scheduleTasks.get(key);
			SchedType taskType = definition.getType();
			String taskName = taskType.getName();
			if (definition.getStatisticName() != null) {
				taskName += "/" + definition.getStatisticName();
			}
			String tableName = definition.getTableName();
			if (tableName == null) {
				tableName = "*";
			}
			String settingsKey = null;
			switch (taskType) {
			case APP_DEFAULT:
			case TABLE_DEFAULT:
				settingsKey = tableName + "/*";
				break;
			default:
				settingsKey = tableName + "/" + taskName;
				break;
			}
			TaskSettings task = TaskSettings.createByKey(settingsKey);
			String schedule = definition.getSchedSpec();
			if (!Utils.isEmpty(schedule)) {
				task.setSchedule(schedule);
			}
			mapTasks.put(task.getKey(), task);
		}
		
		// Extract info about all the other tasks
		// Currently we don't need any information about "not manageable" tasks
		// in the Doradus console, so excluding the next section of code.
//		for (TaskId task : TaskDBUtils.getLiveTasks(appName)) {
//			String key = task.getTaskTableId().substring(appName.length() + 1);
//			if (!mapTasks.containsKey(key)) {
//				StringBuilder settingsKey = new StringBuilder(task.getTableName() == null ? "*" : task.getTableName());
//				settingsKey.append("/").append(task.getTaskType());
//				if (task.getParam() != null) {
//					settingsKey.append("/").append(task.getParam());
//				}
//				mapTasks.put(key, TaskSettings.createByKey(settingsKey.toString()));
//			}
//		}
		return mapTasks;
	}

	@Override
	public TaskStatus getTaskStatus(String appName, String taskKey) {
		return TaskDBUtils.getTaskStatus(appName, taskKey);
	}

	@Override
	public void interrupt() throws IOException {
		interrupt(TaskFilter.NO_FILTER);
	}

	@Override
	public void interrupt(final String finAppName) throws IOException {
		interrupt(new TaskFilter() {
			@Override
			public boolean filter(String appName, String taskId) {
				return appName.equals(finAppName);
			}
		});
	}

	@Override
	public boolean interrupt(final String finAppName, final String taskKey) {
		return interrupt(new TaskFilter() {
			@Override
			public boolean filter(String appName, String taskId) {
				return new TaskMatcher(finAppName, taskKey).match(appName, taskId);
			}
		});
	}

	@Override
	public void suspend() throws IOException {
		suspend(TaskFilter.NO_FILTER);
	}

	@Override
	public void suspend(final String finAppName) throws IOException {
		suspend(new TaskFilter() {
			@Override
			public boolean filter(String appName, String taskId) {
				return appName.equals(finAppName);
			}
		});
	}

	@Override
	public void suspend(final String finAppName, final String taskKey) {
		suspend(new TaskFilter() {
			@Override
			public boolean filter(String appName, String taskId) {
				return new TaskMatcher(finAppName, taskKey).match(appName, taskId);
			}
		});
	}

	@Override
	public void resume() throws IOException {
		resume(TaskFilter.NO_FILTER);
	}

	@Override
	public void resume(final String finAppName) throws IOException {
		resume(new TaskFilter() {
			@Override
			public boolean filter(String appName, String taskId) {
				return appName.equals(finAppName);
			}
		});
	}

	@Override
	public void resume(final String finAppName, final String taskKey) {
		resume(new TaskFilter() {
			@Override
			public boolean filter(String appName, String taskId) {
				return new TaskMatcher(finAppName, taskKey).match(appName, taskId);
			}
		});
	}

	@Override
	public void shutdown(boolean waitForTasksToComplete) throws IOException {
		if (!waitForTasksToComplete) {
			interrupt();
		}
		stopService();
	}

	@Override
	public void updateSettings(String appName, TaskSettings settings)
			throws IOException {
		String scheduleName = getScheduleName(appName, settings);
		String taskName = settings.getTaskName();
		String taskId = settings.getKey();
		String[] taskIdParts = taskId.split("/");
		SchedType taskType = 
				taskName != null ? SchedType.getByName(taskIdParts[1]) :
				settings.getTableName() == null ? SchedType.APP_DEFAULT : 
				SchedType.TABLE_DEFAULT;
		ApplicationDefinition oldAppDef = SchemaService.instance().getApplication(appName);
		oldAppDef.getSchedules().remove(scheduleName);
		if (settings.getSchedule() != null ||
				(taskType != SchedType.APP_DEFAULT && taskType != SchedType.TABLE_DEFAULT)) {
			oldAppDef.addSchedule(new ScheduleDefinition(
					oldAppDef, taskType,
					settings.getSchedule(),
					settings.getTableName(),
					taskIdParts.length <= 2 ? "" : taskIdParts[2]));
		}
		SchemaService.instance().defineApplication(oldAppDef);
	}

	@Override
	public void startScheduling() throws IOException {
		// Not used in current implementation
	}

	@Override
	public TaskStatus getTaskInfo(String appName, String taskName) {
		return TaskDBUtils.getTaskStatus(appName, taskName);
	}

	@Override
	public void setTaskInfo(String appName, String taskName, TaskStatus status) {
		TaskDBUtils.setTaskStatus(appName, taskName, status);
	}

	@Override
	public boolean startImmediately(String appName, String taskId) {
		DoradusTask task = DoradusTask.createTask(appName, taskId);
		if (task != null) {
			m_scheduler.launch(task);
		}
		return task != null;
	}
	
	public void startTask(DoradusTask task) {
		m_scheduler.launch(task);
	}
	
    /**
     * Creates a name for task scheduling according to the task settings
     * @param appName   Application name
     * @param settings  Task settings
     * @return          Task name for scheduling
     */
    public static String getScheduleName(String appName, TaskSettings settings) {
        if(settings.isDefaultSettings()) {
            StringBuffer scheduleName = new StringBuffer();
            if(settings.getTableName() == null || settings.getTableName().isEmpty()) {
                scheduleName.append("app-default");
                scheduleName.append(TaskSettings.KEY_SEP);
                scheduleName.append(appName);
            }
            else {
                scheduleName.append("table-default");
                scheduleName.append(TaskSettings.KEY_SEP);
                scheduleName.append(appName);
                scheduleName.append(TaskSettings.KEY_SEP);
                scheduleName.append(settings.getTableName());   
            }
            return scheduleName.toString();
        }
        String taskName = settings.getKey();
        StringBuffer scheduleName = new StringBuffer(getTaskType(taskName));
        scheduleName.append(TaskSettings.KEY_SEP);
        scheduleName.append(appName);
        String tableName = settings.getTableName();
        if( tableName != null && !tableName.isEmpty()) {
            scheduleName.append(TaskSettings.KEY_SEP);
            scheduleName.append(tableName);
        }
        else {
            scheduleName.append(TaskSettings.KEY_SEP);
            scheduleName.append(TaskSettings.KEY_ALL);          
        }
        if(taskName != null && !getTaskDeclaration(taskName).isEmpty()) {
            scheduleName.append(TaskSettings.KEY_SEP);
            scheduleName.append(getTaskDeclaration(taskName));          
        }
        return scheduleName.toString();
    }
    
    /**
     * Task name has the form table/type/params.
     * This function extracts the task parameters part (if exist).
     * @param taskName
     * @return
     */
    private static String getTaskDeclaration(String taskName) {
        String[] nameParts = taskName.split("/");
        if (nameParts.length <= 2) return "";
        return nameParts[2];
    }
    
    /**
     * Task name has the form table/type/params. This function extracts the task type.
     * @param taskName
     * @return
     */
    private static String getTaskType(String taskName) {
        String[] nameParts = taskName.split("/");
        return nameParts[1];
    }
    
	public static void migrateTasksInfo() {
		Map<String, Map<String, TaskStatus>> taskMap = TaskDBUtils.getAllOldTasks();
		for (Map.Entry<String, Map<String, TaskStatus>> entry : taskMap.entrySet()) {
			for (Map.Entry<String, TaskStatus> taskEntry : entry.getValue().entrySet()) {
				TaskDBUtils.setTaskStatus(entry.getKey(), taskEntry.getKey(), taskEntry.getValue());
			}
		}
	}
	
	/**
	 * Selects tasks by a given filter and interrupts them.
	 * @param filter
	 */
	private boolean interrupt(TaskFilter filter) {
		boolean taskFound = false;
		Map<String, Set<String>> taskNames = TaskDBUtils.getAllTaskNames();
		for (String appName : taskNames.keySet()) {
			for (String taskId : taskNames.get(appName)) {
				if (filter.filter(appName, taskId)) {
					boolean interrupted = TaskDBUtils.interruptTask(appName, taskId);
					taskFound = taskFound || interrupted;
				}
			}
		}
		return taskFound;
	}
	
	/**
	 * Selects tasks by a given filter and suspends them.
	 * @param filter
	 */
	private boolean suspend(TaskFilter filter) {
		boolean taskFound = false;
		Map<String, Set<String>> taskNames = TaskDBUtils.getAllDefinedTasks();
		for (String appName : taskNames.keySet()) {
			for (String taskId : taskNames.get(appName)) {
				if (filter.filter(appName, taskId)) {
					TaskDBUtils.suspendTask(appName, taskId);
					taskFound = true;
				}
			}
		}
		return taskFound;
	}
	
	/**
	 * Selects tasks by a given filter and resumes them.
	 * @param filter
	 */
	private boolean resume(TaskFilter filter) {
		boolean taskFound = false;
		Map<String, Set<String>> taskNames = TaskDBUtils.getAllDefinedTasks();
		for (String appName : taskNames.keySet()) {
			for (String taskId : taskNames.get(appName)) {
				if (filter.filter(appName, taskId)) {
					TaskDBUtils.resumeTask(appName, taskId);
					taskFound = true;
				}
			}
		}
		return taskFound;
	}
    
	/**
	 * Starts a thread that periodically tests whether a running task
	 * should be stopped. External commands (e.g. from Doradus console
	 * or from REST interface command) may claim for interruption.
	 */
    private void startCheckTaskInterruptions() {
    	long timeInterval = ServerConfig.getInstance().check_interrupting_interval;
    	ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    	executor.scheduleAtFixedRate(new Runnable() {
    		@Override
    		public void run() {
				for (TaskExecutor te : m_scheduler.getExecutingTasks()) {
					if (te.isStopped()) {
						// Task was already stopped
						continue;
					}
					DoradusTask task = (DoradusTask)te.getTask();
					TaskStatus status = TaskDBUtils.getTaskStatus(task.getAppName(), task.getTaskId());
					if (status.getLastRunState() == TaskRunState.Interrupting && te.isAlive() && !te.isStopped()) {
						te.stop();
					}
				}
    		}
    	}, 
    	timeInterval, timeInterval, TimeUnit.SECONDS);
    }
}
