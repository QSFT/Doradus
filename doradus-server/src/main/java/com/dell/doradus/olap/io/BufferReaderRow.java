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

public class BufferReaderRow implements IBufferReader {
	private StorageHelper m_helper;
	private String m_app;
	private String m_row;
	private FileInfo m_info;

    public BufferReaderRow(StorageHelper helper, String app, String row, FileInfo info) {
    	m_helper = helper;
    	m_app = app;
    	m_row = row;
    	m_info = info;
    }
    
	@Override public byte[] readBuffer(int bufferNumber) {
		byte[] buffer = null;
		if(m_info.getSingleRow()) {
			buffer = m_helper.readFileChunk(m_app, m_row, "Data/" + m_info.getName() + "/" + bufferNumber);
		} else if(m_info.getSharesRow()) {
			buffer = m_helper.readFileChunk(m_app, m_row + "/_share", m_info.getName() + "/" + bufferNumber);
		} else {
			buffer = m_helper.readFileChunk(m_app, m_row + "/" + m_info.getName(), "" + bufferNumber);
		}
		if(!m_info.getUncompressed()) {
			buffer = Compressor.uncompress(buffer);
		}
        if(buffer == null) throw new RuntimeException("End of stream");
        return buffer;
	}

}
