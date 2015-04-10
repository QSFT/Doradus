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

package com.dell.doradus.utilities;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

public class MemoryAppender extends AppenderSkeleton {
	private static Object m_sync = new Object();
	private static List<LoggingEvent> m_current = new ArrayList<LoggingEvent>(20000);
	private static List<LoggingEvent> m_backup = new ArrayList<LoggingEvent>(20000);
	private static Layout m_layout;
	
	private int m_capacity = 20000;
	
	public MemoryAppender() {}
	
	public void setCapacity(int capacity) {
		m_capacity = capacity;
		synchronized(m_sync) {
			m_current = new ArrayList<LoggingEvent>(m_capacity);
			m_backup = new ArrayList<LoggingEvent>(m_capacity);
		}
	}
	public int getCapacity() { return m_capacity; }
	
	@Override public void activateOptions() {
		m_layout = this.layout;
		super.activateOptions();
	};
	
	@Override public void close() {
		if(this.closed) return;
		this.closed = true;
	}

	@Override public boolean requiresLayout() { return true; }

	@Override protected void append(LoggingEvent event) {
		synchronized(m_sync) {
			if(m_current.size() > m_capacity) {
				List<LoggingEvent> temp = m_current;
				m_current = m_backup;
				m_backup = temp;
				m_current.clear();
			}
			m_current.add(event);
		}
	}
	
	public static String getLog(String level) {
		Level lvl = level == null ? Level.DEBUG : Level.toLevel(level);
		StringBuilder sb = new StringBuilder();
		synchronized(m_sync) {
			for(LoggingEvent event: m_backup) {
				if(event.getLevel().toInt() < lvl.toInt()) continue;
				String log = m_layout.format(event);
				sb.append(log);
			}
			sb.append("\n");
			for(LoggingEvent event: m_current) {
				if(event.getLevel().toInt() < lvl.toInt()) continue;
				String log = m_layout.format(event);
				sb.append(log);
			}
			return sb.toString();
		}
	}
}
