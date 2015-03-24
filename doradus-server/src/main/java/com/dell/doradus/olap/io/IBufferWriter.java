package com.dell.doradus.olap.io;

public interface IBufferWriter {
	public void writeBuffer(int bufferNumber, byte[] buffer, int length);
	public void close(long length);
}
