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

public class InMemoryBufferWriter implements IBufferWriter {
    private List<byte[]> m_buffers = new ArrayList<>();
    private long m_length;
    
    public InMemoryBufferWriter() {}
    
    @Override public void writeBuffer(int bufferNumber, byte[] buffer, int length) {
    	if(bufferNumber != m_buffers.size()) throw new RuntimeException("Error!");
    	byte[] buf = new byte[length];
    	System.arraycopy(buffer, 0, buf, 0, length);
    	buf = Compressor.compress(buf);
    	m_buffers.add(buf);
	}
    
    @Override public void close(long length) { m_length = length; }

    public List<byte[]> getData() { return m_buffers; }
    public long getLength() { return m_length; }
    public long getCompressedLength() {
        long length = 0;
        for(byte[] buffer: m_buffers) {
            length += buffer.length;
        }
        return length;
    }
}
