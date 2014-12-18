package com.dell.doradus.olap.io;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.Utils;

public class DataCache {
	private static int m_maxCachedColumns = 200;
	
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
	
	public DataCache(String storeName, String row, StorageHelper helper) {
		m_storeName = storeName;
		m_row = row;
		m_helper = helper;
	}

	public void addInfo(FileInfo info) {
		if(m_cachedInfos == null) m_cachedInfos = new ArrayList<FileInfo>(m_maxCachedColumns); 
		m_cachedInfos.add(info);
		if(m_cachedInfos.size() >= m_maxCachedColumns) {
			flushCachedInfos();
		}
	}

	public void addData(FileInfo info, byte[] data) {
		if(m_cachedData == null) m_cachedData = new ArrayList<FileData>(m_maxCachedColumns); 
		m_cachedData.add(new FileData(info, data));
		if(m_cachedData.size() >= m_maxCachedColumns) {
			flushCachedData();
		}
	}
	
	public void flush() {
		flushCachedData();
		flushCachedInfos();
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
		//m_helper.write(m_storeName, m_row + "/_share", list);
		m_cachedData.clear();
	}
	
}
