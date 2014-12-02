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

// Virtual directory storage for OLAP
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
		List<ColumnValue> list = m_helper.get(m_storeName, m_row, "Directory/");
		List<String> result = new ArrayList<String>(list.size());
		for(ColumnValue v : list) {
			result.add(v.columnName);
		}
		return result;
	}
	
	public List<FileInfo> listFiles() {
		String prefix = "File/";
		List<ColumnValue> list = m_helper.get(m_storeName, m_row, prefix);
		List<FileInfo> result = new ArrayList<FileInfo>(list.size());
		for(ColumnValue v : list) {
			result.add(new FileInfo(v.columnName, v.getString()));
		}
		return result;
	}

	public boolean directoryExists(String name) {
		byte[] b = m_helper.getValue(m_storeName, m_row, "Directory/" + name);
		return b != null;
	}

	public boolean fileExists(String file) {
		byte[] b = m_helper.getValue(m_storeName, m_row, "File/" + file);
		return b != null;
	}
	
	public long totalLength(boolean recursive) {
		List<FileInfo> list = listFiles();
		long totalLength = 0;
		for(FileInfo f : list) totalLength += f.getLength();
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
			List<FileInfo> childFiles = listFiles();
			//first, detach directory from parent to make delete transactional
			m_helper.delete(m_storeName, m_parent.m_row, "Directory/" + m_name);
			for(FileInfo child : childFiles) {
				if(!child.getSharesRow()) {
					m_helper.delete(m_storeName, m_row + "/" + child.getName());
				}
			}
			m_helper.delete(m_storeName, m_row + "/_share");
			
			List<String> childDirs = listDirectories();
			for(String child : childDirs) getDirectory(child).delete();
			
			m_helper.delete(m_storeName, m_row);
			m_logger.debug("Deleted {}", m_row);
		}
	}

	
	// ************** FILES ************** //
	
	public FileInfo getFileInfo(String file) {
		byte[] val = m_helper.getValue(m_storeName, m_row, "File/" + file);
		if(val == null) return null;
		FileInfo info = new FileInfo(file, Utils.toString(val));
		return info;
	}
	
	public long fileLength(String file) {
		FileInfo info = getFileInfo(file);
		if(info == null) return -1;
		return info.getLength();
	}

	public long compressedLength(FileInfo file) {
		if(file.getUncompressed()) return file.getLength();
		long compressedLength = 0;
		int i = 0;
		byte[] val = null;
		while(true) {
			if(file.getSharesRow()) {
				val = m_helper.getValue(m_storeName, m_row + "/_share", file.getName() + "/" + i);
			} else {
				val = m_helper.getValue(m_storeName, m_row + "/" + file.getName(), "" + i);
			}
			if(val == null) break;
			compressedLength += val.length;
			i++;
		}
		return compressedLength;
	}
	
	public VInputStream open(String name) {
		return open(name, true);
	}
	
	public VInputStream open(String name, boolean useCache) {
		FileInfo info = getFileInfo(name);
    	if(info == null) {
    		throw new FileDeletedException("File '" + name + "' does not exist in '" + m_storeName + "/" + m_row + "'");
    	}
		IBufferReader bufferReader = new BufferReaderRow(m_helper, m_storeName, m_row, info, useCache);
		VInputStream stream = new VInputStream(bufferReader, info.getLength());
		return stream;
	}

	public VOutputStream create(String name) {
		return create(name, true);
	}
	
	public VOutputStream create(String name, boolean useCache) {
		IBufferWriter bufferWriter = new BufferWriterRow(m_helper, m_storeName, m_row, name, useCache);
		VOutputStream stream = new VOutputStream(bufferWriter);
		return stream;
	}

	public String readAllText(String file) {
		VInputStream stream = open(file, false);
		byte[] b = new byte[(int)stream.length()];
		stream.read(b, 0, b.length);
		return Utils.toString(b);
	}
	
	public void writeAllText(String file, String text) {
		VOutputStream stream = create(file, false);
		byte[] b = Utils.toBytes(text);
		stream.write(b, 0, b.length);
		stream.close();
	}
	
	public void deleteFile(String name) {
		m_helper.delete(m_storeName, m_row + "/" + name);		
	}
	
}
