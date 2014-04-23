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

package com.dell.doradus.management;

import java.beans.ConstructorProperties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Java bean class whose instances count the min, average, max, and total
 * sum of registered time intervals.
 */
public class TimeCounter {

	private final AtomicLong count = new AtomicLong(0);
	private final AtomicLong total = new AtomicLong(0);
	private final AtomicLong min = new AtomicLong(0);
	private final AtomicLong max = new AtomicLong(0);

	public TimeCounter() {
	}

	@ConstructorProperties({ "count", "total", "min", "max" })
	public TimeCounter(long count, long total, long min, long max) {
		this.count.set(count);
		this.total.set(total);
		this.min.set(min);
		this.max.set(max);
	}

	/**
	 * Adds the difference between current time and a given origin time
	 * specified in nanoseconds.
	 **/
	public void addAfter(long originInNanos) {
		long end = System.nanoTime();
		addMicro((end - originInNanos) / 1000);
	}

	/** Adds interval given in nanoseconds **/
	public void addNano(long nanos) {
		addMicro(nanos / 1000);
	}

	/** Adds interval given in microseconds **/
	public synchronized void addMicro(long micros) {
		total.addAndGet(micros);

		if (count.incrementAndGet() == 1) {
			min.set(micros);
			max.set(micros);
		} else {
			if (micros < min.get()) {
				min.set(micros);
			}
			if (micros > max.get()) {
				max.set(micros);
			}
		}
	}

	/**
	 * @return The total number of values added to this counter.
	 */
	public long getCount() {
		return count.get();
	}

	/**
	 * @return The mean value added to this counter, in microseconds.
	 */
	public long getAvg() {
		return (long) Math.ceil((double) total.get() / count.get());
	}

	/**
	 * @return The max value added to this counter, in microseconds.
	 */
	public long getMax() {
		return max.get();
	}

	/**
	 * @return The min value added to this counter, in microseconds.
	 */
	public long getMin() {
		return min.get();
	}

	/**
	 * @return The total sum of values added to this counter, in microseconds.
	 */
	public long getTotal() {
		return total.get();
	}

	/**
	 * Zeroizes out this counter.
	 */
	public synchronized void reset() {
		count.set(0L);
		total.set(0L);
		min.set(0L);
		max.set(0L);
	}

	/**
	 * Clones this counter and zeroizes out it afterwards if the 'reset' is
	 * true.
	 * 
	 * @param reset
	 *            zero out this counter
	 * @return clone of this counter
	 */
	public synchronized TimeCounter snapshot(boolean reset) {
		TimeCounter s = new TimeCounter();

		if (reset) {
			s.count.set(count.getAndSet(0));
			s.total.set(total.getAndSet(0));
			s.min.set(min.getAndSet(0));
			s.max.set(max.getAndSet(0));
		} else {
			s.count.set(count.get());
			s.total.set(total.get());
			s.min.set(min.get());
			s.max.set(max.get());
		}
		return s;
	}

	public String toStr() {
		StringBuilder b = new StringBuilder();
		b.append("count: " + count.get() + "\n");
		b.append("max:   " + max.get() + "\n");
		b.append("min:   " + min.get() + "\n");
		b.append("avg:   " + getAvg() + "\n");
		b.append("total: " + total.get() + "\n");
		return b.toString();
	}
}
