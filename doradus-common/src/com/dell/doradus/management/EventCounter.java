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
 * Calculates the number of events.
 */
public class EventCounter {

	private final AtomicLong count = new AtomicLong(0);

	public EventCounter() {
	}

	@ConstructorProperties({ "count" })
	public EventCounter(long count) {
		this.count.set(count);
	}

	/** Increments this counter by one */
	public void increment() {
		count.incrementAndGet();
	}

	/** Decrements this counter by one */
	public void decrement() {
		count.decrementAndGet();
	}

	/** Current count */
	public long getCount() {
		return count.get();
	}

	/**
	 * Zeroizes out this counter.
	 */
	public void reset() {
		count.set(0L);
	}

	/**
	 * Clones this counter and zeroizes out it afterwards if the 'reset' is
	 * true.
	 * 
	 * @param reset
	 *            zero out this counter
	 * @return clone of this counter
	 */
	public EventCounter snapshot(boolean reset) {
		EventCounter s = new EventCounter();
		if (reset) {
			s.count.set(count.getAndSet(0));
		} else {
			s.count.set(count.get());
		}
		return s;
	}

	public String toStr() {
		return "" + count.get();
	}
}
