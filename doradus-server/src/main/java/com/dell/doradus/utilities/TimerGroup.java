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
import java.util.Collections;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class logs the operation execution time.
 * Time spans are measured in nanoseconds. Output is truncated to milliseconds.
 */
public class TimerGroup {
	private boolean m_condition;
	private Logger m_logger;
	
	long m_startTime;
	long m_nextLog;
	long m_logIntervalNano;
	
	private TimerGroupItem m_total = new TimerGroupItem("TOTAL");
	
	HashMap<String,TimerGroupItem> m_timers = new HashMap<String, TimerGroupItem>();
	HashMap<String, Counter> m_counters = new HashMap<String, Counter>();

	/**
	 * Create TimerGroup.
	 */
	public TimerGroup(Logger logger, long logIntervalNano) {
		m_logger = logger;
		m_condition = m_logger.isDebugEnabled();
		m_logIntervalNano = logIntervalNano;
		m_startTime = System.nanoTime();
		if (m_logIntervalNano == 0){
			m_nextLog = Long.MAX_VALUE;
		}else {
			m_nextLog = m_startTime + m_logIntervalNano;
		}
	}

	public TimerGroup(Logger logger) {
		this(logger, 0);
	}

	public TimerGroup(String loggerName, long logIntervalNano) {
		this(LoggerFactory.getLogger(loggerName), logIntervalNano);
	}

	public TimerGroup(String loggerName) {
		this(LoggerFactory.getLogger(loggerName));
	}

	public void start(String name){
		if (m_condition){
			long time = System.nanoTime();
			start(time, name);
			checkLog(time);
		}
	}

	public void start(String name, String details){
			if (m_condition){
				long time = System.nanoTime();
	//			start(time, name);
				start(time, getDetailsName(name,details));
				checkLog(time);
			}
		}

	public long stop(String name){
		if (m_condition){
			long time = System.nanoTime();			
			long elapsedTime = stop(time, name);
			checkLog(time);
			return elapsedTime;
		}
		return 0;
	}

	public long stop(String name, int value){
		if (m_condition){
			long time = System.nanoTime();			
			long elapsedTime = stop(time, name, value);
			checkLog(time);
			return elapsedTime;
		}
		return 0;
	}

	public long stop(String name, String details){
			if (m_condition){
				long time = System.nanoTime();
	//			stop(time, name);
				long elapsedTime = stop(time, getDetailsName(name,details));
				checkLog(time);
				return elapsedTime;
			}
			return 0;
		}

	public long stop(String name, String details, int value){
			if (m_condition){
				long time = System.nanoTime();
	//			stop(time, name,value);
				long elapsedTime = stop(time, getDetailsName(name,details),value);
				checkLog(time);
				return elapsedTime;
			}
			return 0;
		}

	public void add(String name, int value){
		if (m_condition){
			name = getName(name);
			Counter counter = m_counters.get(name);
			if (counter == null){
				counter = new Counter();
				m_counters.put(name, counter);
			}
			counter.add(value);
			checkLog(System.nanoTime());
		}
	}

	public void log(String format, Object... args) {
		log(true, format, args);
	}

	public void log() {
		log(true, null);
	}

	/** 
	 * Start the named timer if the condition is true.
	 */
	private void start(long time, String name){
		name = getName(name);
		TimerGroupItem timer = m_timers.get(name);
		if (timer == null){
			timer = new TimerGroupItem(name);
			m_timers.put(name, timer);
		}
		timer.start(time);
		m_total.start(time);
	}

	private String getDetailsName(String name, String details){
		return name + " / " + details;
	}
	
	private String getName(String name){
		return "(" + Thread.currentThread().getName() + ") " + name;
	}
	/** 
	 * Stop the named timer if the condition is true.
	 */
	private long stop(long time, String name){
		m_total.stop(time);
		TimerGroupItem timer = m_timers.get(getName(name));
		long elapsedTime = 0;
		if (timer != null){
			elapsedTime = timer.stop(time);
		}
		checkLog(time);
		return elapsedTime;
	}

	private long stop(long time, String name, int value){
		m_total.stop(time);
		TimerGroupItem timer = m_timers.get(getName(name));
		long elapsedTime = 0;
		if (timer != null){
			elapsedTime = timer.stop(time, value);
		}
		checkLog(time);
		return elapsedTime;
	}

	/** 
	 * Log elapsed time of all named timers if the condition is true.
	 */
	private void log(boolean finalLog, String format, Object... args) {
		if (m_condition){
			if (format != null ){
				m_logger.debug(String.format(format, args));
			}
			ArrayList<String> timerNames = new ArrayList<String>(m_timers.keySet());
			Collections.sort(timerNames);
			for (String name : timerNames){
				TimerGroupItem timer = m_timers.get(name);
				if (finalLog || timer.changed()){
					m_logger.debug(timer.toString(finalLog));
				}
			}		
			m_logger.debug(m_total.toString(finalLog));
			ArrayList<String> counterNames = new ArrayList<String>(m_counters.keySet());
			Collections.sort(counterNames);
			for (String name : counterNames){
				Counter counter = m_counters.get(name);
				if (finalLog || counter.changed()){
					String text = String.format("%s: (%s)", name, counter.toString(finalLog));
					m_logger.debug(text);
				}
			}		
		}
	}

	private void checkLog(long time) {
		if (m_nextLog <= time){
			log(false, "Intermediate timing ... %s (%s)", Timer.toString(time - m_startTime), Timer.toString(time - m_nextLog));
			m_nextLog = time + m_logIntervalNano;
		}
	}
}

class Counter {
	int m_value;
	int m_lastValue;
	void add(int value){
		m_value +=value;
	}
	
	int value(){
		return m_value;
	}
	
	boolean changed(){
		return m_value != m_lastValue;
	}
	
	public String toString(boolean finalLog){
		m_lastValue = m_value;
		return Integer.toString(finalLog? m_value : m_value - m_lastValue);
	}
}

class TimerGroupItem{

	String m_name;
	Timer m_timer;
	int m_count;
	int m_value;
	boolean m_hasValue;
	int m_lastCount;
	int m_lastValue;
	long m_lastTime;

	TimerGroupItem(String name){
		m_name = name;
		m_timer = new Timer();
	}
	
	public void start(long time) {
		m_count++;
		m_timer.start(time);
	}
		
	public long stop(long time, int value){
		m_value += value;
		m_hasValue = true;
		return m_timer.stop(time);
	}
	
	public long stop(long time){
		return m_timer.stop(time);
	}
	
	boolean changed(){
		return (m_hasValue && m_lastCount != m_count);
	}
	
	public String toString(boolean finalLog){
		long time = m_timer.getElapsedTime();
		int count = m_count;
		int value = m_value;
		
		String text;
		if (!finalLog){
			if (m_hasValue){
				text = String.format("%s: %s/%s (%d/%d) %d/%d", m_name, Timer.toString(time), Timer.toString(time-m_lastTime), count, count - m_lastCount, value, value - m_lastValue);
			}
			else {
				text = String.format("%s: %s/%s (%d/%d)", m_name, Timer.toString(time), Timer.toString(time-m_lastTime), count, count - m_lastCount);
			}
		} else {
			if (m_hasValue){
				text = String.format("%s: %s (%d) %d", m_name, Timer.toString(time), count, value);
			}
			else {
				text = String.format("%s: %s (%d)", m_name, Timer.toString(time), count);
			}
		}
		m_lastTime = time;
		m_lastCount = count;
		m_lastValue = value;
		return text;	
	}
}
