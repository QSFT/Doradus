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

public class InMemoryBufferReader implements IBufferReader {
    private List<byte[]> m_buffers = new ArrayList<>();

    public InMemoryBufferReader(List<byte[]> buffers) { m_buffers = buffers; }
    
	@Override public byte[] readBuffer(int bufferNumber) {
        if(bufferNumber >= m_buffers.size()) throw new RuntimeException("End of stream");
		byte[] buffer = m_buffers.get(bufferNumber);
		buffer = Compressor.uncompress(buffer);
        return buffer;
	}

}
