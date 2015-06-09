/*
 * Copyright (C) 2014 Dell, Inc.
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

package com.dell.doradus.logservice.store;

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.collections.MemoryStream;

public class TimestampBuilder {
	private long[] m_timestamps;
	
	public TimestampBuilder(int size) {
	    m_timestamps = new long[size];
	}
	
	public void add(int doc, String timestamp) {
	    long ts = Utils.parseDate(timestamp).getTimeInMillis();
		m_timestamps[doc] = ts;
	}

	public void flush(MemoryStream output, Temp temp) {
	    long last = 0;
	    MemoryStream s_st = temp.getStream(0);
        for(int i = 0; i < m_timestamps.length; i++) {
            long next = m_timestamps[i];
            s_st.writeVLong(next - last);
            last = next;
        }
        Temp.writeCompressed(output, s_st);
	}

    public int getSize() { return m_timestamps.length; }

	public long getMinTimestamp() {
	    long timestamp = m_timestamps[0];
	    for(int i = 1; i < m_timestamps.length; i++) {
	        if(m_timestamps[i] < timestamp) timestamp = m_timestamps[i];
	    }
	    return timestamp;
	}
	
    public long getMaxTimestamp() {
        long timestamp = m_timestamps[0];
        for(int i = 1; i < m_timestamps.length; i++) {
            if(m_timestamps[i] > timestamp) timestamp = m_timestamps[i];
        }
        return timestamp;
    }
	
}
