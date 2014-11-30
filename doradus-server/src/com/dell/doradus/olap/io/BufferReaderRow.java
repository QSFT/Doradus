package com.dell.doradus.olap.io;

public class BufferReaderRow implements IBufferReader {
	private StorageHelper m_helper;
	private String m_app;
	private String m_row;
	private FileInfo m_info;
    private boolean m_useCache = true;

    public BufferReaderRow(StorageHelper helper, String app, String row, FileInfo info, boolean useCache) {
    	m_helper = helper;
    	m_app = app;
    	m_row = row;
    	m_info = info;
    	m_useCache = useCache;
    }
    
	@Override public byte[] readBuffer(int bufferNumber) {
		byte[] buffer = null;
		if(m_info.getSharesRow()) {
			buffer = m_helper.readFileChunk(m_app, m_row + "/_share", m_info.getName() + "/" + bufferNumber, m_useCache, m_info.getUncompressed());
		} else {
			buffer = m_helper.readFileChunk(m_app, m_row + "/" + m_info.getName(), "" + bufferNumber, m_useCache, m_info.getUncompressed());
		}
        if(buffer == null) throw new RuntimeException("End of stream");
        return buffer;
	}

}
