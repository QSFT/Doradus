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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.Utils;

public class FileIO implements IO {
    private static Logger LOG = LoggerFactory.getLogger(FileIO.class);
    private File main;
	
	public FileIO() {
		main = new File("C:/DoradusDB");
		if(!main.exists()) main.mkdir();
	}
	
	@Override public byte[] getValue(String app, String key, String column) {
		File file = new File(main.getAbsoluteFile() + "/" + app + "/" + key + "/" + column.replace('/', '@'));
		if(!file.exists()) return null;
		try {
			FileInputStream fis = new FileInputStream(file);
			byte[] b = new byte[(int)file.length()];
			fis.read(b, 0, b.length);
			fis.close();
			return b;
		} catch (IOException e) {
			LOG.error("Error reading file", e);
			throw new RuntimeException("Error reading file", e);
		}
	}
	
	@Override public List<ColumnValue> get(String app, String key, String prefix) {
		prefix = prefix.replace('/', '@');
		File dir = new File(main.getAbsoluteFile() + "/" + app + "/" + key);
		List<ColumnValue> result = new ArrayList<ColumnValue>();
		if(!dir.exists()) return result;
		File[] files = dir.listFiles();
		for(File file: files) {
			String column = file.getName();
			if(!column.startsWith(prefix)) continue;
			byte[] value = getValue(app, key, column);
			result.add(new ColumnValue(column.substring(prefix.length()).replace('@', '/'), value));
		}
		return result;
	}
	
	@Override public void createCF(String name) {
		File dir = new File(main.getAbsoluteFile() + "/" + name);
		if(!dir.exists()) dir.mkdir();
	}

	@Override public void deleteCF(String name) {
		File dir = new File(main.getAbsoluteFile() + "/" + name);
		if(dir.exists()) Utils.deleteDirectory(dir);
	}
	
	@Override public void write(String app, String key, List<ColumnValue> values) {
		File dir = new File(main.getAbsoluteFile() + "/" + app + "/" + key);
		if(!dir.exists()) dir.mkdirs();
		for(ColumnValue v : values) {
			File file = new File(dir, v.columnName.replace('/', '@'));
			try {
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(v.columnValue);
			fos.close();
			}catch(IOException e) {
				LOG.error("Error writing file", e);
				throw new RuntimeException("Error writing file", e);
			}
		}
	}
	
	@Override public void delete(String columnFamily, String sKey, String columnName) {
		if(columnName == null) {
			File dir = new File(main.getAbsoluteFile() + "/" + columnFamily + "/" + sKey);
			if(dir.exists()) Utils.deleteDirectory(dir);
		} else {
			File file = new File(main.getAbsoluteFile() + "/" + columnFamily + "/" + sKey + "/" + columnName.replace('/', '@'));
			if(file.exists()) file.delete();
		}
	}

}
