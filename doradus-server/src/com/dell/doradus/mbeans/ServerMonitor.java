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

package com.dell.doradus.mbeans;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.management.EventRate;
import com.dell.doradus.management.RequestsTracker;
import com.dell.doradus.management.ServerMonitorMXBean;

/**
 * Implements the ServerMonitorMXBean interface and provides methods for
 * registration of beginning and completion of request processing by running
 * <i>Doradus</>-server instance. It is server code developer responsibility
 * to call these methods when it is required.
 * 
 * <p>
 * <b>NOTE:</b> The constructors of this class is not intended for direct usage.
 * Instead, use {@code getServerMonitor} static method of the {@code MBeanProvider} 
 * class.
 */
public class ServerMonitor extends MBeanBase implements ServerMonitorMXBean {

	private long startTime;
	private String version;
	private RequestsTracker allRequestsTracker;
	private RequestsTracker recentRequestsTracker;
	private AtomicInteger currentConnections = new AtomicInteger(0);
	private int databaseLink = -1;
	private ScheduledExecutorService exeService;
	private MeterMetric meter;


	/**
	 * Creates new ServerMonitor instance and optionally registers
	 * it on the platform MBeanServer.
	 * @param publish True, if you want to register the created instance. 
	 * Otherwise, nonpublic bean will be constructed.
	 */
	public ServerMonitor(boolean publish) {
		this.domain = JMX_DOMAIN_NAME;
		this.type = JMX_TYPE_NAME;
		
		if(publish) {
			register();
		}
		
		startTime = System.currentTimeMillis();
		version = getClass().getPackage().getImplementationVersion();
		allRequestsTracker = new RequestsTracker();
		recentRequestsTracker = new RequestsTracker();
		exeService = Executors.newSingleThreadScheduledExecutor();
		meter = MeterMetric.newMeter(exeService, "requests", TimeUnit.SECONDS);
	}
	
	public void shutdown() {
		exeService.shutdown();
	}

	/**
	 * @return The start time of server, in milliseconds.
	 */
	@Override
	public long getStartTime() {
		return startTime;
	}

	/**
	 * @return The server's release version.
	 */
	@Override
	public String getReleaseVersion() {
		return version;
	}

	/**
	 * @return The server's configuration properties.
	 */
	@Override
	public Map<String, String> getServerConfig() {
		Map<String, Object> src = ServerConfig.getInstance().toMap();
		Map<String, String> map = new HashMap<String, String>();
		for (String k : src.keySet()) {
			map.put(k, "" + src.get(k));
		}
		return map;
	}
	
	/**
     * Returns an integer that indicates location type of database server 
     * linked to the running Doradus-server: {@code LOCAL} if the Doradus-server 
     * and database-server are hosted on the same machine, {@code REMOTE} if
     * they are on the different machines, {@code UNKNOWN} if no IP address for 
     * the configured hostname of  database-server could be found. 
	 * @return {@code LOCAL}, {@code REMOTE}, or {@code UNKNOWN}
	 */
	public int getDatabaseLink() {
		if(databaseLink < 0) {
			ServerConfig c = ServerConfig.getInstance();
	
			try {
				boolean[] local = new boolean[1];
				String dbhost = extractValidHostname(c.dbhost, local);
				databaseLink = local[0] ? LOCAL : REMOTE;
				
				logger.info("Database hostname: " + dbhost + " (local=" + local[0] + ").");
				
			} catch (UnknownHostException ex) {
				logger.warn(ex.getMessage());
				databaseLink = UNKNOWN;
			}			
		}
		return databaseLink;
	}

	/**
	 * The statistics of requests that was accumulated during all period of
	 * operating time of the server.
	 * 
	 * @return The RequestsTracker instance.
	 */
	@Override
	public RequestsTracker getAllRequests() {
		return allRequestsTracker;
	}

	/**
	 * The statistics of the requests that was accumulated after previous read
	 * of this attribute.
	 * 
	 * @return The RequestsTracker instance.
	 */
	@Override
	public RequestsTracker getRecentRequests() {
		return recentRequestsTracker.snapshot(true);
	}
	/**
	 * @return
	 */
	@Override
	public EventRate getThroughput() {
		return meter.toEventRate();
	}
	
	/**
	 * The number of opened connections to the server.
	 */
	@Override
	public int getConnectionsCount() {
		return currentConnections.get();
	}

	/**
	 * @return
	 */
	@Override
	public String getWorkingDirectory() {
		return System.getProperty("user.dir");
	}

	// //////////////////////////////////////////////////////

	/** Sets the start time of server, in milliseconds. */
	public void setStartTime(long milliseconds) {
		this.startTime = milliseconds;
	}

	/** Sets the server's release version. */
	public void setReleaseVersion(String version) {
		this.version = version;
	}
	
	/** Registers new connection. */
	public void onConnectionOpened() {
		currentConnections.incrementAndGet();
	}
	
	/** Registers a closed or timed-out connection. */
	public void onConnectionClosed() {
		currentConnections.decrementAndGet();
	}

	/**
	 * Registers new request received by server.
	 */
	public synchronized void onNewRequest() {
		allRequestsTracker.onNewRequest();
		recentRequestsTracker.onNewRequest();
	}

	/**
	 * Registers the successful completion of execution of request.
	 * 
	 * @param startTimeInNanos
	 *            The start-time of execution, in nanoseconds.
	 */
	public synchronized void onRequestSucceeded(long startTimeInNanos) {
		long micros = (System.nanoTime() - startTimeInNanos) / 1000;
		allRequestsTracker.onRequestSucceeded(micros);
		recentRequestsTracker.onRequestSucceeded(micros);
		meter.mark();
	}

	/**
	 * Registers an invalid request rejected by the server.
	 */
	public synchronized void onRequestRejected(String reason) {
		allRequestsTracker.onRequestRejected(reason);
		recentRequestsTracker.onRequestRejected(reason);
		meter.mark();
	}

	/**
	 * Registers a failed request.
	 */
	public synchronized void onRequestFailed(String reason) {
		allRequestsTracker.onRequestFailed(reason);
		recentRequestsTracker.onRequestFailed(reason);
		meter.mark();
	}

	/**
	 * Registers a failed request.
	 */
	public synchronized void onRequestFailed(Throwable reason) {
		allRequestsTracker.onRequestFailed(reason);
		recentRequestsTracker.onRequestFailed(reason);
		meter.mark();
	}

	/**
	 * Registers a failed request.
	 */
	public synchronized void onRequestFailed(String message, Throwable ex) {
		String reason = message + ": " + ex.getClass().getName() + ": " + ex.getMessage();
		allRequestsTracker.onRequestFailed(reason);
		recentRequestsTracker.onRequestFailed(reason);
		meter.mark();
	}
	
	public static String extractValidHostname(String hostNameList, boolean[] localFound) throws UnknownHostException {
		boolean x = localFound != null && localFound.length > 0;
		
		if(x) localFound[0] = false;

		if(hostNameList != null) {
			hostNameList = hostNameList.trim();
			
			if(!"".equals(hostNameList)) {
				String[] arr = hostNameList.split("[,;]");
				for(int i = 0; i < arr.length; i++) {
					try {
						InetAddress a = InetAddress.getByName(arr[i]);
						
						if(x) {
							if(a.isLoopbackAddress()) {
								localFound[0] = true;
							} else {
							    try {
							    	localFound[0] = NetworkInterface.getByInetAddress(a) != null;
							    } catch (SocketException e) {
							    }
							}
						}

						return arr[i];
						
					} catch (UnknownHostException ex) {
					}
				}
				
				throw new UnknownHostException("No valid IP address could be found in the specification: \"" + hostNameList + "\".");
			}
		}
		
		if(x) localFound[0] = true;
		return "127.0.0.1";
	}
}
