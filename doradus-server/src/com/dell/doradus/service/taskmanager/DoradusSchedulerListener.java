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

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.tasks.DoradusTask;

import it.sauronsoftware.cron4j.SchedulerListener;
import it.sauronsoftware.cron4j.TaskExecutor;

/**
 * Class implements behavior during background tasks starting and finishing.
 */
public class DoradusSchedulerListener implements SchedulerListener {

    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
	/**
	 * On task start: nothing special to do
	 */
	@Override
	public void taskLaunching(TaskExecutor te) {}

	/**
	 * On task finish: update task status as succeeded or interrupted depending
	 * on finished task status
	 */
	@Override
	public void taskSucceeded(TaskExecutor te) {
		String status = te.getStatusMessage();
		if (!Utils.isEmpty(status)) {
			TaskDBUtils.finishTask((DoradusTask)te.getTask());
		}
	}

	/**
	 * On task fail: tries to manually re-execute the task if only a limit of
	 * unsuccessful executions is not exceeded. If exceeded marks task status as failed.
	 */
	@Override
	public void taskFailed(TaskExecutor te, Throwable reason) {
		m_logger.info("Finished abnormally... ");
		DoradusTask task = (DoradusTask)te.getTask();
		if (task.getFails() < ServerConfig.getInstance().task_restarts) {
			m_logger.info("               ...restarting");
			task.addFail();
			te.getScheduler().launch(task);
		} else {
			m_logger.info("               ...no more restarts");
			TaskDBUtils.finishAbnormallyTask(task);
		}
	}

}
