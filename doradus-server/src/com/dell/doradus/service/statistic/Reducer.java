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

import java.util.AbstractMap;

import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.Aggregate.AverageStatistic;
import com.dell.doradus.search.aggregate.Aggregate.StatisticResult;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.spider.SpiderService;

/**
 * Updates statistic information in the database.
 */
public class Reducer {
	public static void reduce(StatisticRunner task) {
		DBService dbService = DBService.instance();
		String appName = task.getAppName();
		String tableName = SpiderService.statsStoreName(appName);

		StatisticResult result = task.getStatistic();

		String fieldName = result.getFieldName();
		DBTransaction transaction = dbService.startTransaction(appName);
		transaction.deleteRow(tableName, fieldName);
		dbService.commit(transaction);

		transaction = dbService.startTransaction(appName);
		for(AbstractMap.SimpleEntry<String, Object> entry : result.getValues()) {
			transaction.addColumn(tableName, fieldName, entry.getKey(), Utils.toBytes(entry.getValue().toString()));
		}
		dbService.commit(transaction);

		if(result instanceof AverageStatistic) {
			AverageStatistic average = (AverageStatistic)result;
			String sumName = average.getSumFieldName();
			transaction = dbService.startTransaction(appName);
			transaction.deleteRow(tableName, sumName);
			dbService.commit(transaction);

			transaction = dbService.startTransaction(appName);
			for(AbstractMap.SimpleEntry<String, Object> entry : average.getSumValues())
			{
				transaction.addColumn(tableName, sumName, entry.getKey(), Utils.toBytes(entry.getValue().toString()));
			}
			dbService.commit(transaction);
		}
	}

}
