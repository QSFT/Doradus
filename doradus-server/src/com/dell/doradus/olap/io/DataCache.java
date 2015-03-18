/*
 * Copyright (C) 2015 Dell, Inc.
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.dell.doradus.common.Utils;

public class DataCache {
	private static int m_maxCachedColumns = 200;
	private List<Future<?>> m_futures = new ArrayList<>();
	
	
	private static class FileData {
		private FileInfo m_fileInfo;
		private byte[] m_data;
		
		public FileData(FileInfo fileInfo, byte[] data) {
			m_fileInfo = fileInfo;
			m_data = data;
		}
		
		public FileInfo getInfo() { return m_fileInfo; }
		public byte[] getData() { return m_data; }
	}
	
	private String m_storeName;
	private String m_row;
	private StorageHelper m_helper;
	private List<FileInfo> m_cachedInfos = null;
	private List<FileData> m_cachedData = null;
	private Object m_syncRoot = new Object();
	
	public DataCache(String storeName, String row, StorageHelper helper) {
		m_storeName = storeName;
		m_row = row;
		m_helper = helper;
	}

	public void addInfo(FileInfo info) {
		synchronized(m_syncRoot) {
			if(m_cachedInfos == null) m_cachedInfos = new ArrayList<FileInfo>(m_maxCachedColumns); 
			m_cachedInfos.add(info);
			if(m_cachedInfos.size() >= m_maxCachedColumns) {
				flushCachedInfos();
			}
		}
	}

	public void addData(FileInfo info, byte[] data) {
		synchronized(m_syncRoot) {
			if(m_cachedData == null) m_cachedData = new ArrayList<FileData>(m_maxCachedColumns); 
			m_cachedData.add(new FileData(info, data));
			if(m_cachedData.size() >= m_maxCachedColumns) {
				flushCachedData();
			}
		}
	}
	
	public void addPendingCompression(Future<?> future) {
		synchronized(m_syncRoot) {
			m_futures.add(future);
		}
	}
	
	public void flush() {
		flushPendingCompressions();
		flushCachedData();
		flushCachedInfos();
	}
	
	private void flushPendingCompressions() {
		if(m_futures.size() == 0) return;
		try {
    		for(Future<?> f: m_futures) f.get();
    		m_futures.clear();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void flushCachedInfos() {
		if(m_cachedInfos == null || m_cachedInfos.size() == 0) return;
		List<ColumnValue> list = new ArrayList<ColumnValue>(m_cachedInfos.size());
		for(FileInfo fi: m_cachedInfos) {
			ColumnValue cv = new ColumnValue("File/" + fi.getName(), Utils.toBytes(fi.asString()));
			list.add(cv);
		}
		m_helper.write(m_storeName, m_row, list);
		m_cachedInfos.clear();
	}

	private void flushCachedData() {
		if(m_cachedData == null || m_cachedData.size() == 0) return;
		List<ColumnValue> list = new ArrayList<ColumnValue>(m_cachedData.size());
		for(FileData d: m_cachedData) {
			ColumnValue cv = new ColumnValue(d.getInfo().getName() + "/0", d.getData());
			list.add(cv);
		}
		m_helper.writeFileChunks(m_storeName, m_row + "/_share", list);
		m_cachedData.clear();
	}
	
}
