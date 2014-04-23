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

import java.util.List;

public interface IO {
	public byte[] getValue(String app, String key, String column);
	public List<ColumnValue> get(String app, String key, String prefix);
	public void createCF(String name);
	public void deleteCF(String name);
	public void write(String app, String key, List<ColumnValue> values);
	public void delete(String columnFamily, String key, String columnName);
}
