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
 * It is an abstract runnable Java bean class whose instances encapsulate code
 * of asynchronous operations executed in context of MXBeans and represent
 * execution status of these operations to JMX clients.
 */
public class LongJob implements Runnable {

	public enum Status {
		READY, RUNNING, SUCCEEDED, FAILED
	}

	public LongJob(String id, String name) {
		status = Status.READY;
		this.id = id;
		this.name = name;
	}

	@ConstructorProperties({ "id", "name", "status", "startTime", "finishTime",
			"progress", "failure" })
	public LongJob(String id, String name, Status status, long startTime, long finishTime,
			String progress, String failure) {
		this.id = id;
		this.name = name;
		this.status = status;
		this.startTime = startTime;
		this.finishTime = finishTime;
		this.progress = progress;
		this.failure = failure;
	}

	/** The job ID. */
	public String getId() {
		return id;
	}

	/** The job name. */
	public String getName() {
		return name;
	}

	/** The job status. */
	public Status getStatus() {
		return status;
	}

	/** The time when job has been started, in milliseconds, or -1. */
	public long getStartTime() {
		return startTime;
	}

	/** The time when job has been finished, in milliseconds, or -1. */
	public long getFinishTime() {
		return finishTime;
	}

	/**
	 * The description of activity which is performed by the running job at
	 * current moment.
	 */
	public String getProgress() {
		return progress;
	}

	/** The description of reason why the job failed, or null. */
	public String getFailure() {
		return failure;
	}

	/**
	 * The current or final time, in milliseconds, during which the job either
	 * is executed or was executed.
	 */
	public long getDuration() {
		Status s = status;
		if (s == Status.READY)
			return 0;
		if (s == Status.RUNNING)
			return System.currentTimeMillis() - startTime;
		return finishTime - startTime;
	}

	/** The true value means that this job has not yet been finished. */
	public boolean isActive() {
		return status == Status.READY || status == Status.RUNNING;
	}

	@Override
	public void run() {
		startTime = System.currentTimeMillis();
		status = Status.RUNNING;
		progress = null;
		failure = null;

		try {
			doJob();
			status = Status.SUCCEEDED;
			progress = null;
		} catch (Exception ex) {
			status = Status.FAILED;
			failure = ex.getClass().getName() + ": " + ex.getMessage();
		} finally {
			finishTime = System.currentTimeMillis();
		}
	}

	/**
         */
	protected void doJob() throws Exception {
		/* (empty) */
	}

	/**
          */
	protected void cancelJob() throws Exception {
		/* (empty) */
	}

	public String toStr() {
		return name + ": " + status + (progress != null ? ": " + progress : "")
				+ (failure != null ? ": " + failure : "");
	}

	public String toJson() {
		StringBuilder b = new StringBuilder();
		b.append("{\n");
		b.append("id:   \"" + id + "\",\n");
		b.append("name: \"" + name + "\",\n");
		b.append("status: " + status + ",\n");
		b.append("startTime: " + startTime + ",\n");
		b.append("terminalTime: " + finishTime + ",\n");
		b.append("progress: \"" + progress + "\",\n");
		b.append("failure: \"" + failure + "\",\n");
		b.append("}\n");
		return b.toString();
	}

	protected String id;
	protected String name;
	protected Status status;
	protected long startTime = -1;
	protected long finishTime = -1;
	protected String progress;
	protected String failure;
}
