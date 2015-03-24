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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It is access point (on server's side) to MBeans/MXBeans which compose
 * a management interface of running <i>Doradus</i>-server instance.
 */
public class MBeanProvider {

	private static final Logger logger = LoggerFactory.getLogger(MBeanProvider.class.getSimpleName());
	private static final Object lock = new Object();
	private static ServerMonitor serverMonitor;
	private static StorageManager storageManager;
	private static boolean populated;

	/**
	 * @return The ServerMonitorMXBean instance, registered on platform
	 *         MBeanServer.
	 */
	public static ServerMonitor getServerMonitor() {
		if (serverMonitor == null) {
			populateMBeanServer();
		}
		return serverMonitor;
	}

	/**
	 * @return The StorageManagerMXBean instance, registered on platform
	 *         MBeanServer.
	 */
	public static StorageManager getStorageManager() {
		if (storageManager == null) {
			populateMBeanServer();
		}
		return storageManager;
	}

	/**
	 * Creates and registers MBeans/MXBeans.
	 */
	public static void populateMBeanServer() {
		if(populated)
			return;
		
		synchronized (lock) {
			populated = true;
			
			if (serverMonitor == null) {
				logger.info("Creating and registering the ServerMonitor MXBean");
				try { serverMonitor = new ServerMonitor(true); } catch(Exception ex) {
					logger.warn("Can't publish ServerMonitor MXBean.", ex);
					logger.warn("Nonpublic instance of the ServerMonitor MXBean will be used in current session.");
					serverMonitor = new ServerMonitor(false);
				}
			}
			if (storageManager == null) {
				logger.info("Creating and registering the StorageManager MXBean");
				try{ storageManager = new StorageManager(true); } catch(Exception ex) {
					logger.warn("Can't publish StorageManager MXBean.", ex);
					logger.warn("Nonpublic instance of the StorageManager MXBean will be used in current session.");
					storageManager = new StorageManager(false);
				}
			}
		}
	}

	/**
	 * Deregisters MBeans/MXBeans.
	 */
	public static void unpopulateMBeanServer() {
		if(!populated)
			return;
		
		synchronized (lock) {
			populated = false;
			
			if (storageManager != null && storageManager.getPublicName() != null) {
				logger.info("Unregistering the StorageManager MXBean");
				try { 
					storageManager.deregister(); 
					storageManager.awaitJobTermination(2 * 60);
				} catch(Exception ex) {
					logger.warn("Can't unregister StorageManager MXBean.", ex);
				}
			}
			if (serverMonitor != null && serverMonitor.getPublicName() != null) {
				logger.info("Unregistering the ServerMonitor MXBean");
				try { 
					serverMonitor.deregister(); 
					serverMonitor.shutdown();
				} catch(Exception ex) {
					logger.warn("Can't unregister ServerMonitor MXBean.", ex);
				}
			}
			
			storageManager = null;
			serverMonitor = null;
		}
	}
}
