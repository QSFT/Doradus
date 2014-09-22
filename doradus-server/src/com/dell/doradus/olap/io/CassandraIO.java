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

package com.dell.doradus.olap.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.StoreTemplate;

public class CassandraIO implements IO {
    private static Object g_createCFLock = new Object();
    
	public CassandraIO() { }

	@Override
	public byte[] getValue(String app, String key, String column) {
		DColumn col = DBService.instance().getColumn(app, key, column);
		if(col == null) return null;
		return col.getRawValue();
	}

	@Override
	public List<ColumnValue> get(String app, String key, String prefix) {
		List<ColumnValue> result = new ArrayList<ColumnValue>();
		Iterator<DColumn> iColumns = DBService.instance().getColumnSlice(app, key, prefix, prefix + "\uFFFF");
		for (int i = 0; iColumns.hasNext() && i < 16000; ++i) {
			DColumn column = iColumns.next();
			result.add(new ColumnValue(column.getName().substring(prefix.length()), column.getRawValue()));
		}
		return result;
	}

	@Override
	public void createCF(String name) {
	    synchronized (g_createCFLock) {
	        DBService.instance().createStoreIfAbsent(new StoreTemplate(name, true));
	    }
	}

	@Override
	public void deleteCF(String name) {
		DBService.instance().deleteStoreIfPresent(name);
	}

	@Override
	public void delete(String columnFamily, String key, String columnName) {
		DBTransaction transaction = DBService.instance().startTransaction();
		if (columnName == null) {
			transaction.deleteRow(columnFamily, key);
		} else {
			transaction.deleteColumn(columnFamily, key, columnName);
		}
		DBService.instance().commit(transaction);
	}

	@Override public void write(String app, String key, List<ColumnValue> values) {
	    DBTransaction transaction = DBService.instance().startTransaction();
	    for(ColumnValue v : values) {
	        transaction.addColumn(app, key, v.columnName, v.columnValue);
	    }
	    DBService.instance().commit(transaction);
	}
	
}
