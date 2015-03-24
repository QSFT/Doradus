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

/**
 * The Java bean class whose instances track the quantity of input requests and
 * accumulate responses latency statistics.
 */
public class RequestsTracker {

	private EventCounter executing;
	private EventCounter rejected;
	private EventCounter failed;
	private EventCounter succeeded;
	private TimeCounter counter;
	private IntervalHistogram histogram;
	private String lastFailureReason;
	private String lastRejectReason;

	public RequestsTracker() {
		executing = new EventCounter();
		rejected = new EventCounter();
		failed = new EventCounter();
		succeeded = new EventCounter();

		counter = new TimeCounter();
		histogram = new IntervalHistogram(100);
	}

	@ConstructorProperties({ "requestsExecuting", "requestsRejected",
			"requestsFailed", "responseLatencyCounter",
			"responseLatencyHistogram", "lastFailureReason", "lastRejectReason" })
	public RequestsTracker(long executing, long rejected, long failed,
			TimeCounter counter, IntervalHistogram histogram,
			String lastFailureReason, String lastRejectReason) {

		this.executing = new EventCounter(executing);
		this.rejected = new EventCounter(rejected);
		this.failed = new EventCounter(failed);

		this.counter = counter.snapshot(false);
		this.succeeded = new EventCounter(counter.getCount());
		this.histogram = histogram.snapshot(false);

		this.lastFailureReason = lastFailureReason;
		this.lastRejectReason = lastRejectReason;
	}

	/**
	 * Registers new request received by server.
	 */
	public synchronized void onNewRequest() {
		executing.increment();
	}

	/**
	 * Registers a failed request.
	 */
	public synchronized void onRequestFailed(String reason) {
		executing.decrement();
		failed.increment();
		lastFailureReason = reason;
	}

	/**
	 * Registers a failed request.
	 */
	public synchronized void onRequestFailed(Throwable reason) {
		executing.decrement();
		failed.increment();
		lastFailureReason = (reason == null) ? null : reason.getClass()
				.getName() + ": " + reason.getMessage();
	}

	/**
	 * Registers an invalid request rejected by server.
	 */
	public synchronized void onRequestRejected(String reason) {
		executing.decrement();
		rejected.increment();
		lastRejectReason = reason;
	}

	/**
	 * Registers the successful completion of execution of request.
	 * 
	 * @param responeLatencyInMicros
	 *            The response latency, in nanoseconds.
	 */
	public synchronized void onRequestSucceeded(long responeLatencyInMicros) {
		executing.decrement();
		succeeded.increment();
		counter.addMicro(responeLatencyInMicros);
		histogram.add(responeLatencyInMicros);
	}

	/** @return The number of requests currently executing. */
	public long getRequestsExecuting() {
		return executing.getCount();
	}

	/** @return The total number of failed requests. */
	public long getRequestsFailed() {
		return failed.getCount();
	}

	/** @return The total number of rejected requests. */
	public long getRequestsRejected() {
		return rejected.getCount();
	}

	/** @return The total number of succeeded requests. */
	public long getRequestsSucceeded() {
		return succeeded.getCount();
	}

	/** @return The total number of requests. */
	public long getRequestsTotal() {
		return executing.getCount() + succeeded.getCount()
				+ rejected.getCount() + failed.getCount();
	}

	/**
	 * @return The counter of latencies of responses on successfully completed
	 *         requests.
	 */
	public TimeCounter getResponseLatencyCounter() {
		return counter;
	}

	/**
	 * @return The histogram of latencies of responses on successfully completed
	 *         requests.
	 */
	public IntervalHistogram getResponseLatencyHistogram() {
		return histogram;
	}

	/**
	 * @return The reason of last reject or null, if either there are no rejects
	 *         or reason was not specified at registration time of last rejected
	 *         request.
	 */
	public String getLastRejectReason() {
		return lastRejectReason;
	}

	/**
	 * @return The reason of last failure or null, if either there are no
	 *         failures or reason was not specified at registration time of last
	 *         failed request.
	 */
	public String getLastFailureReason() {
		return lastFailureReason;
	}

	/**
	 * Clones this tracker and zeroizes out it afterwards if the 'reset' is
	 * true.
	 * 
	 * @param reset
	 *            zero out this tracker
	 * @return clone of this tracker
	 */
	public synchronized RequestsTracker snapshot(boolean reset) {
		long t = executing.getCount();
		long r = rejected.getCount();
		long f = failed.getCount();
		TimeCounter c = counter.snapshot(reset);
		IntervalHistogram h = histogram.snapshot(reset);

		if (reset) {
			executing.reset();
			rejected.reset();
			failed.reset();
			succeeded.reset();
		}

		return new RequestsTracker(t, r, f, c, h, lastFailureReason,
				lastRejectReason);
	}

	/**
	 * Zeroizes out this tracker.
	 */
	public synchronized void reset() {
		executing.reset();
		rejected.reset();
		failed.reset();
		succeeded.reset();

		counter.reset();
		histogram.reset();

		lastFailureReason = null;
		lastRejectReason = null;
	}

	public String toStr() {
		StringBuilder b = new StringBuilder();
		b.append("executing:  " + executing.getCount() + "\n");
		b.append("rejected:   " + rejected.getCount() + "\n");
		b.append("failed:     " + failed.getCount() + "\n");
		b.append("succeeded:  " + succeeded.getCount() + "\n");
		b.append("total:      " + getRequestsTotal() + "\n");
		b.append("lastFailureReason:  " + lastFailureReason + "\n");
		b.append("lastRejectReason:   " + lastRejectReason + "\n");
		b.append("//\n");
		b.append("ResponseLatencyCounter: \n");
		b.append(counter.toStr());
		b.append("//\n");
		b.append("ResponseLatencyHistogram: \n");
		b.append(histogram.toStr());
		return b.toString();
	}
}
