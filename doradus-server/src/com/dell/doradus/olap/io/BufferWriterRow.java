package com.dell.doradus.olap.io;

import com.dell.doradus.common.Utils;

public class BufferWriterRow implements IBufferWriter {
	private StorageHelper m_helper;
	private String m_app;
	private String m_row;
    private boolean m_useCache = true;
    
    private FileInfo m_info;
    
    public BufferWriterRow(StorageHelper helper, String app, String row, String name, boolean useCache) {
    	m_helper = helper;
    	m_app = app;
    	m_row = row;
    	m_useCache = useCache;
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
    	if(m_info.getSharesRow()) {
    		m_helper.writeFileChunk(m_app, m_row + "/_share", m_info.getName() + "/" + bufferNumber, buf, m_useCache, m_info.getUncompressed());
    	} else {
    		m_helper.writeFileChunk(m_app, m_row + "/" + m_info.getName(), "" + bufferNumber, buf, m_useCache, m_info.getUncompressed());
    	}
	}
	
    @Override public void close(long length) {
    	m_info.setLength(length);
    	m_helper.write(m_app, m_row, "File/" + m_info.getName(), Utils.toBytes(m_info.asString()));
	}

}
