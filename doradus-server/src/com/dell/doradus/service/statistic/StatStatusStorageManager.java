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

import java.util.Iterator;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;
import com.dell.doradus.service.statistic.TaskInfo.Status;

/**
 * Provides persistent for statistic refreshing task status.
 */
public class StatStatusStorageManager {
	/**
	 * Suffix of the row key
	 */
	static private final String STAT_ROW_SUFFIX = "_status"; 
	
	//--------------------------------------------------------
	// Column names
	//--------------------------------------------------------

	static private final String COL_NAME_STATUS = "status"; 
	static private final String COL_NAME_ERR_MESSAGE = "errMessage"; 
	static private final String COL_NAME_TIME_DECLARED = "timeDeclared"; 
	static private final String COL_NAME_TIME_STARTED = "timeStarted"; 
	static private final String COL_NAME_TIME_MAPPED = "timeMapped"; 
	static private final String COL_NAME_TIME_FINISHED = "timeFinished";
	static private final String COL_NAME_TIME_LIVE_STAMP = "timeLiveStamp";
	
	/**
	 * Marks refreshing task status as "live" status with a current timestamp.
	 * We assume the task is dead if it is older then 3 * task_exec_delay seconds.
	 * 
	 * @param appName	Application name
	 * @param tabName	Table name
	 * @param statName	Statistic name
	 */
	public static void stamp(String appName, String tabName, String statName) {
		DBService dbService = DBService.instance();
		ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
		DBTransaction transaction = dbService.startTransaction(Tenant.getTenant(appDef));
		String storeName = SpiderService.statsStoreName(appName);
		String rowKey = tabName + "/" + statName + "/" + STAT_ROW_SUFFIX;
		transaction.addColumn(
				storeName, rowKey, COL_NAME_TIME_LIVE_STAMP,
				Utils.toBytes(Long.toString(System.currentTimeMillis())));
		dbService.commit(transaction);
	}
	
	/**
	 * Persists current task status and puts "live" stamp.
	 * 
	 * @param appName	Application name
	 * @param tabName	Table name
	 * @param statName	Statistic name
	 * @param taskInfo	Task status information.
	 */
	public static void put(String appName, String tabName, String statName, TaskInfo taskInfo) {
		DBService dbService = DBService.instance();
        ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
		DBTransaction transaction = dbService.startTransaction(Tenant.getTenant(appDef));
		String storeName = SpiderService.statsStoreName(appName);
		String rowKey = tabName + "/" + statName + "/" + STAT_ROW_SUFFIX;
		transaction.addColumn(
				storeName, rowKey, COL_NAME_STATUS,
				Utils.toBytes(taskInfo.taskStatus.name()));
		if (taskInfo.errorMessage != null) {
			transaction.addColumn(
					storeName, rowKey, COL_NAME_ERR_MESSAGE,
					Utils.toBytes(taskInfo.errorMessage));
		}
		if (taskInfo.timeDeclared != 0) {
			transaction.addColumn(
					storeName, rowKey, COL_NAME_TIME_DECLARED,
					Utils.toBytes(Long.toString(taskInfo.timeDeclared)));
		}
		if (taskInfo.timeStarted != 0) {
			transaction.addColumn(
					storeName, rowKey, COL_NAME_TIME_STARTED,
					Utils.toBytes(Long.toString(taskInfo.timeStarted)));
		}
		if (taskInfo.timeMapped != 0) {
			transaction.addColumn(
					storeName, rowKey, COL_NAME_TIME_MAPPED,
					Utils.toBytes(Long.toString(taskInfo.timeMapped)));
		}
		if (taskInfo.timeFinished != 0) {
			transaction.addColumn(
					storeName, rowKey, COL_NAME_TIME_FINISHED,
					Utils.toBytes(Long.toString(taskInfo.timeFinished)));
		}
		transaction.addColumn(
				storeName, rowKey, COL_NAME_TIME_LIVE_STAMP,
				Utils.toBytes(Long.toString(taskInfo.timeLiveStamp = System.currentTimeMillis())));
		dbService.commit(transaction);
	}
	
	/**
	 * Extracts current task status from the database.
	 * 
	 * @param appName	Application name
	 * @param tabName	Table name
	 * @param statName	Statistic name
	 * @return			Task status information
	 */
	public static TaskInfo get(String appName, String tabName, String statName) {
		DBService dbService = DBService.instance();
        ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
		Iterator<DColumn> colIterator =
		    dbService.getAllColumns(Tenant.getTenant(appDef), SpiderService.statsStoreName(appName),
		                            tabName + "/" + statName + "/" + STAT_ROW_SUFFIX);
		if (colIterator == null) return null;
		TaskInfo taskInfo = new TaskInfo();
		while (colIterator.hasNext()) {
			DColumn column = colIterator.next();
			switch (column.getName()) {
			case COL_NAME_STATUS:
				taskInfo.taskStatus = Status.valueOf(column.getValue());
				break;
			case COL_NAME_ERR_MESSAGE:
				taskInfo.errorMessage = column.getValue();
				break;
			case COL_NAME_TIME_DECLARED:
				taskInfo.timeDeclared = Long.parseLong(column.getValue());
				break;
			case COL_NAME_TIME_STARTED:
				taskInfo.timeStarted = Long.parseLong(column.getValue());
				break;
			case COL_NAME_TIME_MAPPED:
				taskInfo.timeMapped = Long.parseLong(column.getValue());
				break;
			case COL_NAME_TIME_FINISHED:
				taskInfo.timeFinished = Long.parseLong(column.getValue());
				break;
			case COL_NAME_TIME_LIVE_STAMP:
				taskInfo.timeLiveStamp = Long.parseLong(column.getValue());
				break;
			}
		}
		if (taskInfo.isRunning() && System.currentTimeMillis() - taskInfo.timeLiveStamp > 15*1000) {
			taskInfo.taskStatus = Status.FAILED;
			taskInfo.errorMessage = "Doradus server was stopped during refreshing";
			if (taskInfo.timeFinished == 0) {
				taskInfo.timeFinished = System.currentTimeMillis();
			}
		}
		return taskInfo;
	}
}
