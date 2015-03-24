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

import com.dell.doradus.common.Utils;

public class ColumnValue implements Comparable<ColumnValue>{
	public String columnName;
	public byte[] columnValue;
	
	public ColumnValue(String name) {
		columnName = name;
	}
	
	public ColumnValue(String name, byte[] value) {
		columnName = name;
		columnValue = value;
	}

	public String getString() { return Utils.toString(columnValue); }
	public long getLong() { return Long.parseLong(getString()); }
	
	public void setString(String value) { columnValue = Utils.toBytes(value); }
	public void setLong(long value) { setString("" + value); }

	@Override public int compareTo(ColumnValue o) {
		return columnName.compareTo(o.columnName);
	}
}
