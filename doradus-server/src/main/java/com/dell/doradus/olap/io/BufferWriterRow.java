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


public class BufferWriterRow implements IBufferWriter {
	private StorageHelper m_helper;
	private String m_app;
	private String m_row;
    private DataCache m_dataCache;
    private Object m_syncRoot = new Object();
    
    private FileInfo m_info;
    
    public BufferWriterRow(DataCache dataCache, StorageHelper helper, String app, String row, String name) {
    	m_dataCache = dataCache;
    	m_helper = helper;
    	m_app = app;
    	m_row = row;
    	m_info = new FileInfo(name);
    	m_info.setSingleRow(true);
    }
    
    @Override public void writeBuffer(int bufferNumber, byte[] buffer, int length) {
    	if(bufferNumber == 0 && length < buffer.length) {
    		if(length < 512) m_info.setUncompressed(true);
    		if(length < 65536 && !m_info.getSingleRow()) m_info.setSharesRow(true);
    	}

    	if(m_dataCache.isMultithreaded() && m_info.isCompressed()) {
    		final int finalBufferNumber = bufferNumber;
    		final byte[] buf = new byte[length];
    		System.arraycopy(buffer, 0, buf, 0, length);
    		m_dataCache.addRunnable(new Runnable() {
				@Override public void run() {
			    	write(finalBufferNumber, buf);
				}
    		});
    		return;
    	}
    	
    	byte[] buf = buffer;
    	if(length != buf.length) {
    		buf = new byte[length];
    		System.arraycopy(buffer, 0, buf, 0, buf.length);
    	}

    	write(bufferNumber, buf);
	}
    
    private void write(int bufferNumber, byte[] buf) {
    	if(m_info.isCompressed()) buf = Compressor.compress(buf);
    	synchronized(m_syncRoot) {
    		m_info.setCompressedLength(m_info.getCompressedLength() + buf.length);
    	}
    	
    	if(m_info.getUncompressed()) {
    		m_dataCache.addData(m_info, buf, bufferNumber);
    		return;
    	}
    	
    	if(m_info.getSingleRow()) {
    		m_helper.writeFileChunk(m_app, m_row, "Data/" + m_info.getName() + "/" + bufferNumber, buf);
    	} else if(m_info.getSharesRow()) {
    		m_helper.writeFileChunk(m_app, m_row + "/_share", m_info.getName() + "/" + bufferNumber, buf);
    	} else {
    		m_helper.writeFileChunk(m_app, m_row + "/" + m_info.getName(), "" + bufferNumber, buf);
    	}
    }
    
	
    @Override public void close(long length) {
    	m_info.setLength(length);
    	m_dataCache.addInfo(m_info);
	}

}
