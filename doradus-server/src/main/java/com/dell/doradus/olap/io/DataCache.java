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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;

public class DataCache {
	private static int m_maxCachedColumns = 200;
	private Object m_staticSyncRoot = new Object();
    private static ExecutorService m_executor;
	
	
	private static class FileData {
		private FileInfo m_fileInfo;
		private byte[] m_data;
		private int m_bufferNumber;
		
		public FileData(FileInfo fileInfo, byte[] data, int bufferNumber) {
			m_fileInfo = fileInfo;
			m_data = data;
			m_bufferNumber = bufferNumber;
		}
		
		public FileInfo getInfo() { return m_fileInfo; }
		public byte[] getData() { return m_data; }
		public int getBufferNumber() { return m_bufferNumber; }
	}
	
	private String m_storeName;
	private String m_row;
	private StorageHelper m_helper;
	private List<FileInfo> m_cachedInfos = null;
	private List<FileData> m_cachedData = null;
	private Object m_syncRoot = new Object();
	private List<Future<?>> m_futures = new ArrayList<>();
	
	public DataCache(String storeName, String row, StorageHelper helper) {
		int threads = ServerConfig.getInstance().olap_compression_threads;
		if(threads > 0) {
	    	synchronized(m_staticSyncRoot) {
	    		if(m_executor == null) {
	    			m_executor = new ThreadPoolExecutor(
	    					threads, threads, 0L, TimeUnit.MILLISECONDS,
	    					new ArrayBlockingQueue<Runnable>(threads),
	    					new ThreadPoolExecutor.CallerRunsPolicy());
	    		}
	    	}
		}
		
		
		m_storeName = storeName;
		m_row = row;
		m_helper = helper;
	}

	public boolean isMultithreaded() { return m_executor != null; }
	
	public void addInfo(FileInfo info) {
		synchronized(m_syncRoot) {
			if(m_cachedInfos == null) m_cachedInfos = new ArrayList<FileInfo>(m_maxCachedColumns); 
			m_cachedInfos.add(info);
			if(m_cachedInfos.size() >= m_maxCachedColumns) {
				flushCachedInfos();
			}
		}
	}

	public void addData(FileInfo info, byte[] data, int bufferNumber) {
		synchronized(m_syncRoot) {
			if(m_cachedData == null) m_cachedData = new ArrayList<FileData>(m_maxCachedColumns); 
			m_cachedData.add(new FileData(info, data, bufferNumber));
			if(m_cachedData.size() >= m_maxCachedColumns) {
				flushCachedData();
			}
		}
	}
	
	public void addRunnable(Runnable runnable) {
		Future<?> future = m_executor.submit(runnable);
		synchronized(m_syncRoot) {
			m_futures.add(future);
			if(m_futures.size() > m_maxCachedColumns) {
				flushPendingTasks();
			}
		}
	}
	
	public void flush() {
		flushPendingTasks();
		flushCachedData();
		flushCachedInfos();
	}
	
	private void flushPendingTasks() {
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
			FileInfo fi = d.getInfo();
			if(fi.getSingleRow()) continue;
			if(!fi.getSharesRow()) throw new RuntimeException("Not supported");
			ColumnValue cv = new ColumnValue(fi.getName() + "/" + d.getBufferNumber(), d.getData());
			list.add(cv);
		}
		m_helper.writeFileChunks(m_storeName, m_row + "/_share", list);
		list.clear();
		for(FileData d: m_cachedData) {
			FileInfo fi = d.getInfo();
			if(!fi.getSingleRow()) continue;
			ColumnValue cv = new ColumnValue("Data/" + fi.getName() + "/" + d.getBufferNumber(), d.getData());
			list.add(cv);
		}
		m_helper.writeFileChunks(m_storeName, m_row, list);
		m_cachedData.clear();
	}
	
}
