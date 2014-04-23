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

/**
 * This class logs the operation execution time.
 * Time spans are measured in nanoseconds. Output is truncated to milliseconds.
 */
public class Timer {
	private long m_elapsedTime;
	private long m_startTime;
	private int m_nesting;
	
	/**
	 * Create and start the timer.
	 */
	public Timer() {
		m_startTime = System.nanoTime();
	}

	/**
	 * Start the timer.
	 */
	public void start(long time){
		if (m_nesting == 0){
			m_startTime = time;
		}
		m_nesting++;
	}

	public void start(){
		start(System.nanoTime());
	}
	
	/**
	 * Stop the timer. If timer was started, update the elapsed time.
	 */
	public long stop(long time) {
		m_nesting--;
		// not started and stopped
		if (m_nesting < 0){
			m_nesting = 0;
		}
		if (m_nesting == 0){
			long elapsedTime = time - m_startTime;
			m_elapsedTime += elapsedTime;
			return elapsedTime;
		}
		return 0;
	}

	public long stop() {
		return stop(System.nanoTime());
	}
	
	/** 
	 * Formats the time span as 's.mmm sec' where 's'-seconds, 'mmm'-milliseconds.
	 */
	public static String toString(long elapsedTime) {
		elapsedTime = (elapsedTime + 500000l) / 1000000l;
		return String.format("%d.%03d sec", elapsedTime / 1000l, elapsedTime % 1000l);
	}
	
	/** 
	 * Formats the time span as 's.mmm sec' where 's'-seconds, 'mmm'-milliseconds.
	 * If the timer was stopped, formats the time span between timer start/timer stop time.
	 * If the timer wasn't stopped, formats the time span between timer start time/current time.
	 */
	@Override
	public String toString() {
		return toString(getElapsedTime());
	}
	
	public long getElapsedTime(){		
		// running or never started
		if (m_nesting > 0 || m_elapsedTime == 0){
			return  m_elapsedTime + System.nanoTime() - m_startTime;
		}		
		return m_elapsedTime;
	}
}
