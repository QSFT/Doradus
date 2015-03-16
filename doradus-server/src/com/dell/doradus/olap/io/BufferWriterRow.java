package com.dell.doradus.olap.io;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;

public class BufferWriterRow implements IBufferWriter {
	private static Object m_staticSyncRoot = new Object();
    private static ExecutorService m_executor;
    
	private StorageHelper m_helper;
	private String m_app;
	private String m_row;
    private DataCache m_dataCache;
    //private List<Future<?>> m_futures = new ArrayList<>();
    
    private FileInfo m_info;
    
    public BufferWriterRow(DataCache dataCache, StorageHelper helper, String app, String row, String name) {
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

    	if(m_executor != null && !m_info.getUncompressed() && !m_info.getSharesRow()) {
    		final int finalBufferNumber = bufferNumber;
    		final byte[] buf = new byte[length];
    		System.arraycopy(buffer, 0, buf, 0, length);
    		Future<?> f = m_executor.submit(new Runnable(){
				@Override public void run() {
			    	write(finalBufferNumber, buf);
				}
    		});
    		//m_futures.add(f);
    		m_dataCache.addPendingCompression(f);
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
    	
    	//if(m_executor != null) {
		//	try {
	    //		for(Future<?> f: m_futures) {
		//				f.get();
	    //		}
		//	} catch (InterruptedException e) {
		//		throw new RuntimeException(e);
		//	} catch (ExecutionException e) {
		//		throw new RuntimeException(e);
		//	}
    	//}
	}

}
