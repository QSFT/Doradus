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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.Utils;

public class VDirectory {
    private static Logger m_logger = LoggerFactory.getLogger("Olap.VDirectory");
	static int CHUNK_SIZE = 1024 * 1024;
	
	private VDirectory m_parent;
	private StorageHelper m_helper;
	private String m_storeName;
	private String m_name;
	private String m_row;
	
	public VDirectory(String storeName) {
		m_parent = null;
		IO io = new CassandraIO();
		//IO io = new FileIO();
		//IO io = new SqlIO();
		//IO io = new MemIO();
		m_helper = new StorageHelper(io);
		m_storeName = storeName;
		m_name = "$root";
		m_helper.createCF(m_storeName);
		m_row = "Directory/" + m_name;
	}
	
	private VDirectory(VDirectory parent, String name) {
		m_parent = parent;
		m_helper = parent.m_helper;
		m_storeName = parent.m_storeName;
		m_name = name;
		m_row = m_parent.m_row + "/" + m_name;
	}

	public VDirectory getParent() { return m_parent; }
	public String getName() { return m_name; }
	public String getStoreName() { return m_storeName; }
	public String getRow() { return m_row; }
	
	// ************** DIRECTORIES ************** //
	
	public VDirectory getDirectoryCreate(String name) {
		VDirectory child = new VDirectory(this, name);
		child.create();
		return child;
	}
	public VDirectory getDirectory(String name) { return new VDirectory(this, name); }

	public List<String> listDirectories() {
		List<ColumnValue> lst = m_helper.get(m_storeName, m_row, "Directory/");
		List<String> result = new ArrayList<String>(lst.size());
		for(ColumnValue v : lst) {
			result.add(v.columnName);
		}
		return result;
	}

	public boolean directoryExists(String name) {
		List<ColumnValue> lst = m_helper.get(m_storeName, m_row, "Directory/" + name);
		return lst.size() == 1 && lst.get(0).columnName.length() == 0;
	}
	
	public long totalLength(boolean recursive) {
		String prefix = "File/";
		List<ColumnValue> list = m_helper.get(m_storeName, m_row, prefix);
		long totalLength = 0;
		for(ColumnValue v : list) totalLength += v.getLong();
		if(recursive) {
			for(String child : listDirectories()) {
				totalLength += getDirectory(child).totalLength(recursive);
			}
		}
		return totalLength;
	}
	
	public void create() {
		if(m_parent == null) return;
		m_helper.write(m_storeName, m_parent.m_row, "Directory/" + m_name, new byte[0]); 
	}
	
	public void delete() {
		if(m_parent == null) {
			m_helper.deleteCF(m_storeName);
			m_logger.info("Deleted CF {}", m_storeName);
		} else {
			List<String> childFiles = listFiles();
			List<String> childDirs = listDirectories();
			//first, detach directory from parent to make delete transactional
			m_helper.delete(m_storeName, m_parent.m_row, "Directory/" + m_name);
			for(String child : childFiles) m_helper.delete(m_storeName, m_row + "/" + child);
			for(String child : childDirs) getDirectory(child).delete();
			m_helper.delete(m_storeName, m_row);
			m_logger.debug("Deleted {}", m_row);
		}
	}

	
	// ************** FILES ************** //
	
	public long fileLength(String file) { 
		String prefix = "File/" + file;
		List<ColumnValue> list = m_helper.get(m_storeName, m_row, prefix);
		if(list.size() == 0 || list.get(0).columnName.length() != 0) return -1;
		else return list.get(0).getLong();
	}

	public long compressedLength(String file) { 
		long compressedLength = 0;
		int i = 0;
		while(true) {
			byte[] val = m_helper.getValue(m_storeName, m_row + "/" + file, "" + i);
			if(val == null) break;
			compressedLength += val.length;
			i++;
		}
		return compressedLength;
	}
	
	public boolean fileExists(String file) {
		return fileLength(file) >= 0;
	}
	
	public List<String> listFiles() {
		String prefix = "File/";
		List<ColumnValue> list = m_helper.get(m_storeName, m_row, prefix);
		List<String> lst = new ArrayList<String>(list.size());
		for(ColumnValue v : list) {
			lst.add(v.columnName);
		}
		return lst;
	}
	
	public VInputStream open(String name) {
		return new VInputStream(m_helper, m_storeName, m_row, name, fileLength(name));
	}

	public VOutputStream create(String name) {
		return new VOutputStream(m_helper, m_storeName, m_row, name);
	}

	public String readAllText(String file) {
		VInputStream stream = open(file);
		stream.useCache = false;
		byte[] b = new byte[(int)stream.length()];
		stream.read(b, 0, b.length);
		return Utils.toString(b);
	}
	
	public void writeAllText(String file, String text) {
		VOutputStream stream = create(file);
		stream.useCache = false;
		byte[] b = Utils.toBytes(text);
		stream.write(b, 0, b.length);
		stream.close();
	}
	
	public void deleteFile(String name) {
		long len = fileLength(name);
		if(len < 0) return;
		m_helper.delete(m_storeName, m_row, "File/" + name);
		int chunks = (int)((len + CHUNK_SIZE - 1) / CHUNK_SIZE); 
		for(int i=0; i<chunks; i++) {
			m_helper.delete(m_storeName, m_row, "Chunk/" + name + "/" + i);
		}
	}
	
}
