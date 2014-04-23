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

package com.dell.doradus.client;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.ContentType;

/**
 * RESTConnector is a class that provides multiple connections to Doradus nodes.
 * It can generate RESTClient objects to different hosts. The class implements
 * a simple round robin strategy for selecting next host for connection.
 * It also provides a snap of connection state (when was connected last time).
 * Usage:
 * <p>
 * <ol>
 * <li>RESTConnector connector = new RESTConnector(port, host1, host2,...);</li>
 * <li>RESTClient client1 = connector.getClient();</li>
 * <li>RESTClient client2 = connector.getClient();</li>
 * <li>Client dClient1 = new Client(client1);</li>
 * <li>Client dClient2 = new Client(client2);</li>
 * <li>...</li>
 * </ol>
 */
public class RESTConnector {
	private ContentType m_defaultContentType = ContentType.APPLICATION_JSON;
	private int m_defaultVersion = 1;
	private boolean m_defaultCompression = false;
	
	/**
	 * Status of the connection (at the time it was created or refused to connect)
	 */
	public static class HostStatus {
		private boolean m_alive;		// Connection was set successfully?
		private Exception m_failReason;	// Reason for connection fail
		
		private HostStatus() { this(true, null); }
		private HostStatus(boolean alive, Exception e) { m_alive = alive; m_failReason = e; }
		
		/**
		 * Was the connection with the given host name set successfully?
		 * 
		 * @return    True if the host for this connection was alive.
		 */
		public boolean isAlive() { return m_alive; }
		
		/**
		 * What was the reason for the connection fail (null if it was successful).
		 * 
		 * @return    Returns the reason for the last connect failure or null if it was successful.
		 */
		public Exception getReason() { return m_failReason; }
	}
	
	/**
	 * Sets default content/accept type that would be set to REST clients by default.
	 * 
	 * @param contentType	Content/accept type
	 */
	public void setAcceptType(ContentType contentType) {
		m_defaultContentType = contentType;
	}
	
	/**
	 * Sets default API version that would be set to REST clients by default.
	 * 
	 * @param version	API version to set
	 */
	public void setAPIVersion(int version) {
		m_defaultVersion = version;
	}
	
	/**
	 * Sets default request compression parameter that would be set to REST clients by default.
	 * 
	 * @param compression	Default compression parameter
	 */
	public void setCompression(boolean compression) {
		m_defaultCompression = compression;
	}
	
	/**
	 * An element of the hosts ring
	 */
	private static class Cell {
		String m_hostName;			// host
		HostStatus m_hostStatus;	// status of last connection to the host
		Cell next;					// next cell in the ring
	}
	
	private Cell m_cellRing;	// ring of cells
	private final int m_port;	// connection port
	
	/**
	 * Registers the port and the host names, creates a cells ring, each cell in its
	 * "undefined" state. No exception thrown.
	 * 
	 * @param port		Connection port
	 * @param hosts		Array of connection host names
	 */
	public RESTConnector(int port, String... hosts) {
		m_port = port;
		for (String host : hosts) {
			addToRing(host);
		}
	}
	
	/**
	 * Tries to connect to the "next" host. If no host is available,
	 * IllegalStateException will be raised.
	 * 
	 * @return	Client for the connection established
	 */
	public RESTClient getClient() {
		return getClient(null);
	}
	
	/**
	 * Tries to connect to the "next" host. If no host is available,
	 * IllegalStateException will be raised.
	 * 
	 * @param sslParams	parameters needed to establish SSL connection
	 * @return			Client for the connection established
	 */
	public RESTClient getClient(SSLTransportParameters sslParams) {
		if (m_cellRing == null) {
			throw new IllegalStateException("Empty pool");
		}
		Cell currentCell = m_cellRing;
		StringBuilder errorMessage = new StringBuilder();
		do {
			try {
				RESTClient client = new RESTClient(sslParams, m_cellRing.m_hostName, m_port);
				m_cellRing.m_hostStatus = new HostStatus();
				client.setAcceptType(m_defaultContentType);
				client.setAPIVersion(m_defaultVersion);
				client.setCompression(m_defaultCompression);
				return client;
			} catch (RuntimeException e) {
				m_cellRing.m_hostStatus = new HostStatus(false, e);
				errorMessage.append("Couldn\'t connect to " + m_cellRing.m_hostName)
							.append("; reason: " + e.getMessage() + "\n");
			} finally {
				m_cellRing = m_cellRing.next;
			}
		} while (currentCell != m_cellRing);
		throw new IllegalStateException("No active connections found\n" + errorMessage);
	}
	
	/**
	 * Generates a map of the last state of the attempts of the connections.
	 * Useful in a situation when a getClient method ended in exception, then this
	 * method can show the reasons of refusals.
	 * 
	 * @return	Map of current connection states.
	 */
	public Map<String, HostStatus> getState() {
		Map<String, HostStatus> map = new HashMap<>();
		if (m_cellRing != null) {
			Cell currentCell = m_cellRing;
			do {
				map.put(currentCell.m_hostName, currentCell.m_hostStatus);
				currentCell = currentCell.next;
			} while (currentCell != m_cellRing);
		}
		return map;
	}
	
	/**
	 * Adds a new host to a cells ring.
	 * @param host	Host name to add
	 */
	private void addToRing(String host) {
		Cell newCell = new Cell();
		newCell.m_hostName = host;
		if (m_cellRing == null) {
			newCell.next = newCell;
		} else {
			newCell.next = m_cellRing.next;
			m_cellRing.next = newCell;
		}
		m_cellRing = newCell;
	}
}
