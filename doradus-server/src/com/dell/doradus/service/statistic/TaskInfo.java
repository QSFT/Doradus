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

package com.dell.doradus.service.statistic;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;

public class TaskInfo {
	public enum Status {
		DEFINED,	// Task was created but not yet started
		STARTED,	// Task had just started
		MAPPED,		// Statistic mapping finished
		REDUCED,	// Statistic reducing finished (and a result is obtained)
		FAILED		// Task failed
	};
	
	Status taskStatus;
	String errorMessage;
	
	long timeDeclared;
	long timeStarted;
	long timeMapped;
	long timeFinished;
	long timeLiveStamp;
	
	public TaskInfo() {}
	
	public TaskInfo(Status status) {
		taskStatus = status;
	}
	
	/**
	 * The task was marked as successfully finished or failed. 
	 * @return
	 */
	public boolean isFinished() 
	{
		return taskStatus == Status.REDUCED || taskStatus == Status.FAILED;
	}
	
	/**
	 * The task is prepared for running or is running
	 * @return
	 */
	public boolean isRunning() {
		return taskStatus == Status.DEFINED || taskStatus == Status.STARTED || taskStatus == Status.MAPPED;
	}
	
	/**
	 * The task has a "fresh" live time stamp.
	 * @return
	 */
	public boolean isFresh() {
		return System.currentTimeMillis() - timeLiveStamp <
			   3 * ServerConfig.getInstance().task_exec_delay * 1000;
	}
	
	/**
	 * Returns a short summary of this task's status as a string,
	 * including relevant time stamps.
	 * 
	 * @return
	 */
	public String getStatus()
    {
        switch (taskStatus)
        {
        case DEFINED:
            return "Scheduled for recalculation";
            
        case STARTED:
        case MAPPED:
            return "Recalculation started at: " + Utils.formatDate(timeStarted);
            
        case REDUCED:
            return "Recalculation finished at: " + Utils.formatDate(timeFinished) +
                   "; recalculation time: " + Utils.formatElapsedTime(timeFinished - timeStarted);
            
        case FAILED:
            return "Recalculation failed at: " + Utils.formatDate(timeFinished) + 
                   "; error message: " + errorMessage;
            
        default:
            return "Unknown status: " + taskStatus.toString();
        }
    }   // getStatus
	
}
