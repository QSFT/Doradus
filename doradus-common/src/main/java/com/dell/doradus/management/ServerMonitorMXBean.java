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

import java.util.Map;

/**
 * Defines the interface of a MXBean that provides the application info and
 * request statistics of running <i>Doradus</i>-server instance.
 */
public interface ServerMonitorMXBean {

	public static final String JMX_DOMAIN_NAME = "com.dell.doradus";
	public static final String JMX_TYPE_NAME = "ServerMonitor";

	public static final int UNKNOWN = 0;
	public static final int LOCAL = 1;
	public static final int REMOTE = 2;

	
	/**
	 * @return The start time of server, in milliseconds.
	 */
	public long getStartTime();

	/**
	 * @return The server's release version.
	 */
	public String getReleaseVersion();

	/**
	 * @return The server's configuration properties.
	 */
	public Map<String, String> getServerConfig();

	/**
     * Returns an integer that indicates location type of database server 
     * linked to the running Doradus-server: {@code LOCAL} if the Doradus-server 
     * and database-server are hosted on the same machine, {@code REMOTE} if
     * they are on the different machines, {@code UNKNOWN} if no IP address for 
     * the configured hostname of  database-server could be found. 
	 * @return {@code LOCAL}, {@code REMOTE}, or {@code UNKNOWN}
	 */
	public int getDatabaseLink();

	/**
	 * The statistics of requests that was accumulated during all period of
	 * operating time of the server.
	 * 
	 * @return The RequestsTracker instance.
	 */
	public RequestsTracker getAllRequests();

	/**
	 * The statistics of the requests that was accumulated after previous read
	 * of this attribute. If client takes this attribute's value every minute (-
	 * for example) he will get answers to questions of type: how many requests
	 * are received/failed/rejected per minute?
	 * 
	 * @return The RequestsTracker instance.
	 */
	public RequestsTracker getRecentRequests();
	
	/**
	 * @return Throughput as an EventRate object.
	 */
	public EventRate getThroughput();
	
	/*
	 * The number of opened connections to the server.
	 */
	public int getConnectionsCount();

	/**
	 * @return The working directory of the server.
	 */
	public String getWorkingDirectory();
}
