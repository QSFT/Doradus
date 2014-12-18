package com.dell.doradus.olap.io;

import com.dell.doradus.common.Utils;

public class BufferWriterRow implements IBufferWriter {
	private StorageHelper m_helper;
	private String m_app;
	private String m_row;
    private DataCache m_dataCache;
    
    private FileInfo m_info;
    
    public BufferWriterRow(DataCache dataCache, StorageHelper helper, String app, String row, String name) {
    	m_dataCache = dataCache;
    	m_helper = helper;
    	m_app = app;
    	m_row = row;
    	m_info = new FileInfo(name);
    }
    
    @Override public void writeBuffer(int bufferNumber, byte[] buffer, int length) {
    	if(bufferNumber == 0) {
    		if(length < 512) m_info.setUncompressed(true);
    		if(length < 65536) m_info.setSharesRow(true);
    	}
    	
    	byte[] buf = buffer;
    	if(length != buf.length) {
    		buf = new byte[length];
    		System.arraycopy(buffer, 0, buf, 0, buf.length);
    	}
    	if(!m_info.getUncompressed()) {
			buf = Compressor.compress(buf);
    	}
    	
    	if(m_info.getSharesRow()) {
    		m_dataCache.addData(m_info, buf);
    	} else {
    		m_helper.writeFileChunk(m_app, m_row + "/" + m_info.getName(), "" + bufferNumber, buf);
    	}
	}
	
    @Override public void close(long length) {
    	m_info.setLength(length);
    	if(m_info.getSharesRow()) {
    		m_dataCache.addInfo(m_info);
    	} else {
        	m_helper.write(m_app, m_row, "File/" + m_info.getName(), Utils.toBytes(m_info.asString()));
    	}
	}

}
