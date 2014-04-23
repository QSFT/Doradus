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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The Java bean class each instance of which represents a frequency histogram
 * of registered time values (- latencies, durations, etc.). It shows how many
 * input values hit into a certain interval (-"bin") of a limited range.
 * <p>
 * The range is specified by number of serial, mutually exclusive, intervals.
 * 0-th interval is [0,1] (- includes the bounds). Each next (i-th) has
 * end-point that is larger than end-point of the (i-1)-th interval in 1.2 times
 * (rounding and removing duplicates). The default number of intervals is equal
 * to 90: [0,1],(1,2], ...,(9,10],(10,12],(12,14],(14,17],(17,20],...,(20924300,
 * 25109160], which will give us a timing resolution from microseconds to 25
 * seconds, with less precision as the numbers get larger.
 */
public class IntervalHistogram {
	// end-points of intervals: 1, 2,...,10,12,14,17,20, etc.
	private long[] bins;
	// hits[i] - number of hits into i-th interval; hits[hits.length-1] - number
	// of overflows.
	private final AtomicLongArray hits;

	/** Constructs new histogram with default number of bins (= 90). **/
	public IntervalHistogram() {
		this(90);
	}

	/** Constructs new histogram with given number of bins **/
	public IntervalHistogram(int binsCount) {
		initBins(binsCount);
		hits = new AtomicLongArray(bins.length + 1);
	}

	/**
	 * Constructs new histogram by given bins and hits (assert hits.length ==
	 * bins.length + 1).
	 */
	@ConstructorProperties({ "bins", "hits" })
	public IntervalHistogram(long[] bins, long[] hits) {
		assert hits.length == bins.length + 1;

		this.bins = bins;
		this.hits = new AtomicLongArray(hits);
	}

	/**
	 * Increments the hits in interval containing given 'value', rounding UP.
	 * 
	 * @param value
	 */
	public void add(long value) {
		int index = Arrays.binarySearch(bins, value);
		if (index < 0) {
			// inexact match, take the first bin higher than value
			index = -index - 1;
		}
		hits.incrementAndGet(index);
	}

	/**
	 * The current values of hits.
	 * <p>
	 * 1) hits[hits.length - 1] - number of overflows
	 * <p>
	 * 2) (hits.length - 2 >= i && i >= 0): hits[i] - number of hits into i-th
	 * interval
	 * 
	 * @return a long[] containing the current hits
	 */
	public long[] getHits() {
		long[] h = new long[hits.length()];
		for (int i = 0; i < hits.length(); i++) {
			h[i] = hits.get(i);
		}
		return h;
	}

	/**
	 * The intervals in form of array of end-points.
	 * <p>
	 * 0-th interval: from 0 to bins[0], including starting and end points.
	 * <p>
	 * i-th interval (i>0): from bins[i-1] to bins[i], end inclusive.
	 * 
	 * @return a long[] containing the end-points of intervals.
	 */
	public long[] getBins() {
		return bins;
	}

	/**
	 * The end-point of range (! starting point is 0, always). It is a max value
	 * that may be added to this histogram without it will be overflowed.
	 * 
	 * @return End of range.
	 */
	public long getEnd() {
		return bins[bins.length - 1];
	}

	/**
	 * The true if this histogram has overflowed -- that is, a value larger that
	 * the end-point of range was added.
	 */
	public boolean isOverflowed() {
		return hits.get(bins.length) > 0;
	}

	/**
	 * The mean histogram value. If the histogram overflowed, returns
	 * Long.MAX_VALUE.
	 */
	public long getMean() {
		int n = bins.length;
		if (hits.get(n) > 0) {
			return Long.MAX_VALUE;
		}

		long cnt = 0;
		long sum = 0;
		for (int i = 0; i < n; i++) {
			cnt += hits.get(i);
			sum += hits.get(i) * bins[i];
		}

		return (long) Math.ceil((double) sum / cnt);
	}

	/**
	 * The starting point of interval that contains the smallest value added to
	 * this histogram.
	 */
	public long getMin() {
		for (int i = 0; i < hits.length(); i++) {
			if (hits.get(i) > 0) {
				return i == 0 ? 0 : 1 + bins[i - 1];
			}
		}
		return 0;
	}

	/**
	 * The end-point of interval that contains the largest value added to this
	 * histogram. If the histogram overflowed, returns Long.MAX_VALUE.
	 */
	public long getMax() {
		int lastBin = hits.length() - 1;
		if (hits.get(lastBin) > 0) {
			return Long.MAX_VALUE;
		}

		for (int i = lastBin - 1; i >= 0; i--) {
			if (hits.get(i) > 0) {
				return bins[i];
			}
		}
		return 0;
	}

	/**
	 * Zeroizes out hits.
	 */
	public void reset() {
		for (int i = 0; i < hits.length(); i++) {
			hits.set(i, 0L);
		}
	}

	/**
	 * Clones this histogram and zeroizes out hits afterwards if the 'reset' is
	 * true.
	 * 
	 * @param reset
	 *            zero out hits
	 * @return clone of this histogram's state
	 */
	public IntervalHistogram snapshot(boolean reset) {
		if (reset) {
			return new IntervalHistogram(bins, getAndResetHits());
		}
		return new IntervalHistogram(bins, getHits());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof IntervalHistogram)) {
			return false;
		}

		IntervalHistogram that = (IntervalHistogram) o;
		return Arrays.equals(getBins(), that.getBins())
				&& Arrays.equals(getHits(), that.getHits());
	}

	public String toStr() {
		StringBuilder b = new StringBuilder();

		b.append("range: [0, " + getEnd() + "]\n");

		b.append("hits: \n");
		int cnt = 0;
		for (int i = 0; i < bins.length; i++) {
			long c = hits.get(i);
			if (c > 0) {
				long left = (i == 0) ? 0 : bins[i - 1];
				long right = bins[i];
				b.append("    (" + left + ", " + right + "]: " + c + "\n");
				cnt += c;
			}
		}
		if (cnt == 0) {
			b.append("    (empty)\n");
		}

		b.append("min:  " + getMin() + "\n");
		b.append("max:  " + getMax() + "\n");
		b.append("mean: " + getMean() + "\n");
		b.append("isOverflowed:  " + isOverflowed() + "\n");

		return b.toString();
	}

	private void initBins(int size) {
		bins = new long[size];
		long last = 1;
		bins[0] = last;
		for (int i = 1; i < size; i++) {
			long next = Math.round(last * 1.2);
			if (next == last) {
				next++;
			}
			bins[i] = next;
			last = next;
		}
	}

	private long[] getAndResetHits() {
		long[] h = new long[hits.length()];
		for (int i = 0; i < hits.length(); i++) {
			h[i] = hits.getAndSet(i, 0L);
		}
		return h;
	}
}
