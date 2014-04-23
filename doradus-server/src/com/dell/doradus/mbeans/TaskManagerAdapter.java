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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.management.LongJob;
import com.dell.doradus.management.TaskRunState;
import com.dell.doradus.management.TaskSettings;
import com.dell.doradus.management.TaskStatus;
import com.dell.doradus.service.taskmanager.TaskManagerService;
import com.dell.doradus.tasks.ITaskManager;


public class TaskManagerAdapter {
	private static final boolean DEBUG = false;
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

	private Set<String> lockedMessages;
	

	public TaskManagerAdapter() {
		lockedMessages =  Collections.synchronizedSet(new HashSet<String>());		
	}

	public LongJob consInterruptJob(String jobID, String jobName,
			final String appName, final String taskOrGroupKey) {
		
		LongJob job = new LongJob(jobID, jobName) {
			@Override
			protected void doJob() throws Exception {
				String msg = "Invoking task-manager: ";
				
				if(appName == null) {
					msg += consOpKey("interrupt");
					logger.info(msg);
					progress = msg;
					getTaskManager().interrupt();
					
				} else if(taskOrGroupKey == null) {
					msg += consOpKey("interrupt", appName);
					logger.info(msg);
					progress = msg;
					getTaskManager().interrupt(appName);
					
				} else {
					msg += consOpKey("interrupt", appName, taskOrGroupKey);
					logger.info(msg);
					progress = msg;
					getTaskManager().interrupt(appName, taskOrGroupKey);					
				}		
			}
		};
		return job;
	}

	public LongJob consSuspendJob(String jobID, String jobName,
			final String appName, final String taskOrGroupKey) {
		
		LongJob job = new LongJob(jobID, jobName) {
			@Override
			protected void doJob() throws Exception {
				String msg = "Invoking task-manager: ";
				
				if(appName == null) {
					msg += consOpKey("suspend");
					logger.info(msg);
					progress = msg;
					getTaskManager().suspend();
					
				} else if(taskOrGroupKey == null) {
					msg += consOpKey("suspend", appName);
					logger.info(msg);
					progress = msg;
					getTaskManager().suspend(appName);
					
				} else {
					msg += consOpKey("suspend", appName, taskOrGroupKey);
					logger.info(msg);
					progress = msg;
					getTaskManager().suspend(appName, taskOrGroupKey);					
				}		
			}
		};
		return job;
	}

	public LongJob consResumeJob(String jobID, String jobName,
			final String appName, final String taskOrGroupKey) {
		
		LongJob job = new LongJob(jobID, jobName) {
			@Override
			protected void doJob() throws Exception {
				String msg = "Invoking task-manager: ";
				
				if(appName == null) {
					msg += consOpKey("resume");
					logger.info(msg);
					progress = msg;
					getTaskManager().resume();
					
				} else if(taskOrGroupKey == null) {
					msg += consOpKey("resume", appName);
					logger.info(msg);
					progress = msg;
					getTaskManager().suspend(appName);
					
				} else {
					msg += consOpKey("resume", appName, taskOrGroupKey);
					logger.info(msg);
					progress = msg;
					getTaskManager().resume(appName, taskOrGroupKey);					
				}		
			}
		};
		return job;
	}

	public LongJob consUpdateJob(String jobID, String jobName,
			final String appName, final TaskSettings taskOrGroupSettings) {
		
		LongJob job = new LongJob(jobID, jobName) {
			@Override
			protected void doJob() throws Exception {
				if(appName == null) {
					throw new IllegalArgumentException("Not null application name must be passed.");
				}
				if(taskOrGroupSettings == null) {
					throw new IllegalArgumentException("Not null task/group settings must be passed.");
				}
				
				String key = taskOrGroupSettings.getKey();
				String msg = "Invoking task-manager: ";				
				msg += consOpKey("update", appName, key);
				
				logger.info(msg);
				progress = msg;
				getTaskManager().updateSettings(appName, taskOrGroupSettings);
			}
		};
		return job;
	}
	
	
	
	public TaskSettings getGlobalDefaultSettings() {
		String opKey = consOpKey("getGlobalDefaultSettings");
		TaskSettings res = null;

		try {
			res = getTaskManager().getGlobalDefaultSettings();
			unlockLogging(opKey);
			
		} catch(Exception ex) {
			throwException(ex, opKey);
			//unreachable
		}
		
		return res;
	}

	public Set<String> getAppNames() {
		String opKey = consOpKey("getAppNames");
		Set<String> res = null;

		try {
			res = getTaskManager().getAppNames();
			unlockLogging(opKey);
			
		} catch(Exception ex) {
			throwException(ex, opKey);
			//unreachable
		}
		
		return res;
	}

	public  Map<String, TaskSettings> getAppSettings(String appName) {
		String opKey = consOpKey("getAppSettings", appName);
		Map<String, TaskSettings> res = null;

		try {
			res = getTaskManager().getAppSettings(appName);
			unlockLogging(opKey);
			
		} catch(Exception ex) {
			throwException(ex, opKey);
			//unreachable
		}
		
		return res;
	}
	
	public TaskStatus getTaskStatus(String appName, String taskKey) {
		String opKey = consOpKey("getTaskStatus", appName, taskKey);
		TaskStatus res = null;
		
		try {
			res = getTaskManager().getTaskStatus(appName, taskKey);
			unlockLogging(opKey);
			
		} catch(Exception ex) {
			throwException(ex, opKey);
			//unreachable
		}
		
		return res;
	}
	

	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	
	private ITaskManager getTaskManager() {
		if(DEBUG)
			return TaskManagerDummy.getInstance();
		
		return TaskManagerService.instance();
	}

	private String consOpKey(String opName, String... args) {
		StringBuilder b = new StringBuilder();
		b.append(opName + "(");
		for(int i = 0; i < args.length; i++) {
			if(i > 0) b.append(", ");
			b.append(args[i]);
		}
		b.append(")");
		return b.toString();
	}
	
	private String consLogMsg(String opKey, Exception ex) {
		return opKey + ": " + ex.getClass().getName() + ": " + ex.getMessage();
	}
	
	private boolean lockLogging(String opKey) {
		if(!lockedMessages.contains(opKey)) {
			lockedMessages.add(opKey);
			return true;
		}
		return false;
	}
	
	private void throwException(Exception ex, String opKey) {
		if (lockLogging(opKey)) {
			logger.error(consLogMsg(opKey, ex));
		}
		throw new RuntimeException(ex.getClass().getName() + ": " + ex.getMessage());
	}
	
	private void unlockLogging(String opKey) {
		lockedMessages.remove(opKey);
	}
	
	
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
}

class TaskManagerDummy implements ITaskManager {
	private static TaskManagerDummy instance;
	
	public static ITaskManager getInstance() {
		if(instance == null) {
			instance = new TaskManagerDummy();
		}
		return instance;
	}

	
	int cnt0 = 0;
	int cnt1 = 1;
	int cnt2 = 0;
	int cnt3 = 0;
	long tm0 = System.currentTimeMillis();
	long tm1 = tm0;
	long tm2 = tm0;
	long tm3 = tm0;
	long delay0 = 100000;
	long delay1 = 50000;
	long delay2 = 40000;
	long delay3 = 30000;
	
	void updateCnt0() {
		long tm = System.currentTimeMillis();
		if(tm - tm0 >= delay0) {
			tm0 = tm;
			cnt0 += 1;
		}
	}
	
	void updateCnt1() {
		long tm = System.currentTimeMillis();
		if(tm - tm1 >= delay1) {
			tm1 = tm;
			cnt1 += 1;
		}
	}
	
	void updateCnt2() {
		long tm = System.currentTimeMillis();
		if(tm - tm2 >= delay2) {
			tm2 = tm;
			cnt2 += 1;
		}
	}
	
	void updateCnt3() {
		long tm = System.currentTimeMillis();
		if(tm - tm3 >= delay3) {
			tm3 = tm;
			cnt3 += 1;
		}
	}

	
	@Override
	public Set<String> getAppNames() {
//		throw new IllegalArgumentException("bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... \nBla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla...");

		
		updateCnt1();

		if ((cnt1 % 4) == 1) {
			return getAppList1();
		} else if ((cnt1 % 4) == 2) {
			return getAppList1();
		} else if ((cnt1 % 4) == 3) {
			return getAppList2();
		} else {
//			return getAppList3();
			return getAppList1();
//			throw new IllegalArgumentException("bla-bla-bla...");
		}
	}

	@Override
	public Map<String, TaskSettings> getAppSettings(String appName) throws IOException {
//		throw new IllegalArgumentException("bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... \nBla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla...");

		updateCnt2();

		if ((cnt2 % 4) == 1) {
			return getAppSettings1(appName);
		} else if ((cnt2 % 4) == 2) {
			return getAppSettings1(appName);
		} else if ((cnt2 % 4) == 3) {
			return getAppSettings2(appName);
		} else {
			//return new HashMap<String, TaskSettings>();
			return getAppSettings1(appName);
			//throw new IllegalArgumentException("bla-bla-bla...");
		}
	}

	@Override
	public TaskSettings getGlobalDefaultSettings() {
		updateCnt0();

		if ((cnt0 % 4) == 1) {
			return getGDS1();
		} else if ((cnt0 % 4) == 2) {
			return getGDS1();
		} else if ((cnt0 % 4) == 3) {
			return getGDS2();
		} else {
			return getGDS3();
		}
	}
		
	@Override
	public TaskStatus getTaskStatus(String appName, String taskKey) {
//		throw new IllegalArgumentException("bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... \nBla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla... bla-bla-bla...");

		updateCnt3();

		if ((cnt3 % 5) == 1) {
			return getTaskStatus1();
		} else if ((cnt3 % 5) == 2) {
			return getTaskStatus2();
		} else if ((cnt3 % 5) == 3) {
			return getTaskStatus3();
		} else if ((cnt3 % 5) == 4) {
			return getTaskStatus4();
		} else {
			return getTaskStatus1();
			//throw new IllegalArgumentException("bla-bla-bla...");
		}
	}

	@Override
	public void interrupt() {
		sleep();
	}


	@Override
	public void suspend() {
		sleep();
	}


	@Override
	public void resume() {
		sleep();
	}


	@Override
	public void interrupt(String appName) {
		sleep();
	}


	@Override
	public void suspend(String appName) {
		sleep();
	}


	@Override
	public void resume(String appName) {
		sleep();
	}


	@Override
	public boolean interrupt(String appName, String taskId) {
		sleep();
		return true;
	}


	@Override
	public void suspend(String appName, String taskId) {
		sleep();
	}


	@Override
	public void resume(String appName, String taskId) {
		sleep();
	}


	@Override
	public void shutdown(boolean waitForTasksToComplete) {
		// TODO Auto-generated method stub		
	}


	@Override
	public void updateSettings(String appName, TaskSettings settings) throws IOException {
		sleep();
	}


	@Override
	public void startScheduling() {
	}


	@Override
	public TaskStatus getTaskInfo(String appName, String taskName) {
		return null;
	}


	@Override
	public void setTaskInfo(String appName, String taskName, TaskStatus status) {
	}


	private void sleep() {
		try {
			Thread.sleep(3000);
		} catch(Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	TaskSettings getGDS1() {
		TaskSettings s = new TaskSettings();
		s.setDescription("GDS-1");
		s.setSchedule("0 0 12 * * ?");
		return s;
	}
	
	TaskSettings getGDS2() {
		TaskSettings s = new TaskSettings();
		s.setDescription("GDS-2");
		s.setSchedule("0 15 10 ? * 6#3"); //"0 15 10 ? * 6#12"); //"0 15 10 ? * 2#4,3#2,6#3"); //"0 0 15 * * ?");
		return s;
	}
	
	TaskSettings getGDS3() {
		TaskSettings s = new TaskSettings();
		s.setDescription("GDS-3");
		return s;
	}



	Set<String> getAppList1() {
		Set<String> set = new HashSet<String>();
		set.add("app2");
		set.add("app1");
		set.add("app4");
		set.add("app3");
		return set;
	}

	Set<String> getAppList2() {
		Set<String> set = new HashSet<String>();
		set.add("app2x");
		set.add("app1");
		set.add("app4");
		set.add("app0");
		return set;
	}

	Set<String> getAppList3() {
		Set<String> set = new HashSet<String>();
		return set;
	}

	

	private Map<String, TaskSettings> getAppSettings1(String appName) {

		TaskSettings s0 = new TaskSettings(null);

		TaskSettings s1 = new TaskSettings("Defaults for table1-tasks",
				"table1");
		s1.setSchedule("0 5,10,30 3/3 * * ?");
		
		TaskSettings s2 = new TaskSettings("A table1-task-1", "table1",
				"t-task?1");
		TaskSettings s3 = new TaskSettings("A table1-task-2", "table1",
				"t-task?2");
		TaskSettings s4 = new TaskSettings("A table1-task-3", "table1",
				"t-task?3");

		TaskSettings s5 = new TaskSettings(null, null, "a-task?1");
		TaskSettings s6 = new TaskSettings(null, null, "a-task?2");
		s6.setSchedule("0 * * * * *");

		TaskSettings s7 = new TaskSettings("Defaults for table2-tasks",
				"table2");
		TaskSettings s8 = new TaskSettings("A table2-task-1", "table2",
				"t-task?1");
		TaskSettings s9 = new TaskSettings("A table2-task-2", "table2",
				"t-task?2");

		Map<String, TaskSettings> map = new HashMap<String, TaskSettings>();
		map.put(s0.getKey(), s0);
		map.put(s1.getKey(), s1);
		map.put(s2.getKey(), s2);
		map.put(s3.getKey(), s3);
		map.put(s4.getKey(), s4);
		map.put(s5.getKey(), s5);
		map.put(s6.getKey(), s6);
		map.put(s7.getKey(), s7);
		map.put(s8.getKey(), s8);
		map.put(s9.getKey(), s9);

		return map;
	}

	private Map<String, TaskSettings> getAppSettings2(String appName) {

		// TaskSettings s0 = new TaskSettings("Defaults for all app-tasks");

		TaskSettings s1 = new TaskSettings(null,
				"table1");
		s1.setSchedule("0 5,10,30 3/3 * * ?");

		TaskSettings s2 = new TaskSettings(null, "table1",
				"t-task?1xxx");
		TaskSettings s3 = new TaskSettings("A table1-task-2", "table1",
				"t-task?2");
		TaskSettings s4 = new TaskSettings("A table1-task-3", "table1",
				"t-task?3");

		// TaskSettings s5 = new TaskSettings("A app-task-1", null,
		// "a-task?1zzz");
		TaskSettings s6 = new TaskSettings(null, null, "a-task?2zzz");

		TaskSettings s7 = new TaskSettings("Defaults for table2-tasks",
				"table2");
		TaskSettings s8 = new TaskSettings("A table2-task-1", "table2",
				"t-task?1");
		TaskSettings s9 = new TaskSettings("A table2-task-2", "table2",
				"t-task?2");
		s9.setSchedule("0 * * * * *");

		Map<String, TaskSettings> map = new HashMap<String, TaskSettings>();
		// map.put(s0.getKey(), s0);
		map.put(s1.getKey(), s1);
		map.put(s2.getKey(), s2);
		map.put(s3.getKey(), s3);
		map.put(s4.getKey(), s4);
		// map.put(s5.getKey(), s5);
		map.put(s6.getKey(), s6);
		map.put(s7.getKey(), s7);
		map.put(s8.getKey(), s8);
		map.put(s9.getKey(), s9);

		return map;
	}


	
	private TaskStatus getTaskStatus1() {
		TaskStatus s = new TaskStatus();
		s.setLastRunState(TaskRunState.Undefined);		
		s.setSchedulingSuspended(false);
		return s;
	}
	
	long startTime = System.nanoTime();
	
	private TaskStatus getTaskStatus2() {
		TaskStatus s = new TaskStatus();
		s.setLastRunState(TaskRunState.Started);
		startTime = System.nanoTime();
		s.setLastRunScheduledStartTime(startTime);
		s.setLastRunActualStartTime(startTime);
		return s;
	}
	
	private TaskStatus getTaskStatus3() {
		TaskStatus s = new TaskStatus();
		s.setLastRunState(TaskRunState.Interrupting);
		s.setLastRunScheduledStartTime(startTime);
		s.setLastRunActualStartTime(startTime);
		return s;
	}
	
	private TaskStatus getTaskStatus4() {
		TaskStatus s = new TaskStatus();
		s.setLastRunState(TaskRunState.Interrupted);
		s.setLastRunScheduledStartTime(startTime);
		s.setLastRunActualStartTime(startTime);
		s.setLastRunFinishTime(System.nanoTime());
		s.setSchedulingSuspended(true);
		return s;
	}

	@Override
	public boolean startImmediately(String appName, String taskId) {
		return false;
		
	}
}
