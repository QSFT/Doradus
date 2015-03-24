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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.management.LongJob;


/**
 * Exposes a reduced JMX-based management interface of Cassandra node.
 */
public class CassandraNode {

	private static final boolean DEBUG = false;
	private static final Logger logger = LoggerFactory
			.getLogger(CassandraNode.class.getSimpleName());
	private static final String SNAPSHOTS_DIR_NAME = "snapshots";
	private static float osversion;
	private static double cassandraVersion = -1.0;

	/** Default JMX port of the Cassandra node (7199). */
	public final int DEFAULT_JMX_PORT = 7199;

	/**
	 * Creates new CassandraNode instance connected to local Cassandra-node via
	 * default JMX-port.
	 */
	public CassandraNode() {
		this(null, 0);
	}

	/**
	 * Creates new CassandraNode instance connected to local Cassandra-node via
	 * specified JMX-port.
	 * 
	 * @param port
	 *            The port number. Set to 0 or negative value to specify default
	 *            port.
	 */
	public CassandraNode(int port) {
		this(null, port);
	}

	/**
	 * Creates new CassandraNode instance connected to Cassandra-node on
	 * specified host via specified JMX-port.
	 * 
	 * @param host
	 *            The host name or ip-address. Set to null or empty value to
	 *            specify local host.
	 * @param port
	 *            The port number. Set to 0 or negative value to specify default
	 *            port.
	 */
	public CassandraNode(String host, int port) {
		if (port <= 0) {
			port = DEFAULT_JMX_PORT;
		}

		this.host = host;
		this.port = port;
		isLocal = isLocalHost();
		lockedMessages =  Collections.synchronizedSet(new HashSet<String>());

		try {
			storageServiceName = new ObjectName(
					"org.apache.cassandra.db:type=StorageService");

			runtimeServiceName = new ObjectName("java.lang:type=Runtime");
			osServiceName = new ObjectName("java.lang:type=OperatingSystem");

		} catch (MalformedObjectNameException ex) {
			logger.error("Program error.", ex);
		}

		String osv = System.getProperty("os.version");
		logger.debug("OS version: " + osv);
		// If OS version is x.y.z, strip .z part(s)
		while (osv.lastIndexOf('.') > osv.indexOf('.')) {
		    osv = osv.substring(0, osv.lastIndexOf('.'));
		}
		osversion = Float.parseFloat(osv);

		setDbtoolProps();
	}

	/**
	 * 
	 * @return
	 */
	public String getOS() {
		try {
			OperatingSystemMXBean p = getOperatingSystemMXBeanProxy();
			return p.getName() + ", " + p.getArch();

		} catch (Exception ex) {
			throwException("getOS failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * The true if the node is in "NORMAL" operation mode.
	 */
	public boolean isNormal() {
		try {
			return "NORMAL".equals(getOperationMode());
		} catch (Exception ex) {
			return false;
		}
	}

	/**
	 * @return The true if node has been started with
	 *         "-Dcassandra-foreground=yes" option.
	 */
	public boolean isInForeground() {
		try {
			Map<String, String> props = getRuntimeMXBeanProxy()
					.getSystemProperties();
			if (props.containsKey("cassandra-foreground")) {
				String fg = props.get("cassandra-foreground");
				return fg != null && "yes".equals(fg.toLowerCase());
			}
			return false;

		} catch (Exception ex) {
			throwException("isInForeground failed.", ex);
			return false; /* unreachable */
		}
	}

	/**
	 * @param jobName
	 * @param snapshotName
	 * @return The LongJob instance.
	 */
	public LongJob consCreateSnapshotJob(String jobID, String jobName,
			String snapshotName) {
		final String sn = snapshotName;

		LongJob job = new LongJob(jobID, jobName) {

			@Override
			protected void doJob() throws Exception {
				progress = "Checking the node is in NORMAL mode...";
				if (isNormal()) {
					progress = "Invoking  the \"takeSnapshot\" on the Casandra's MBean...";
					takeSnapshot(sn, new String[0]);
				} else {
					throw new IllegalStateException(
							"Cassandra-node is not in NORMAL operation mode. Try later, please.");
				}
			}
		};
		return job;

	}

	/**
	 * @param jobName
	 * @param snapshotName
	 * @return The LongJob instance.
	 */
	public LongJob consDeleteSnapshotJob(String jobID, String jobName,
			String snapshotName) {
		final String sn = snapshotName;

		LongJob job = new LongJob(jobID, jobName) {

			@Override
			protected void doJob() throws Exception {
				progress = "Checking the node is in NORMAL mode...";
				if (isNormal()) {
					progress = "Invoking  the \"clearSnapshot\" on the Casandra's MBean...";
					clearSnapshot(sn, new String[0]);
				} else {
					throw new IllegalStateException(
							"Cassandra-node is not in NORMAL operation mode. Try later, please.");
				}
			}
		};
		return job;

	}
	
	
	// 1.1.5 => 001001.5
	// 1.2.0 => 001002.0
	// 1.7.3-SNAPSHOT => 001007.3
	// 12.23.345bla-bla => 012023.345
	double getVersionNumber() {
		if(cassandraVersion > 0) {
			return cassandraVersion;
		}
		
		String version = getReleaseVersion();

		if (version == null) {
			throw new IllegalStateException("Can't get Cassandra release version.");
		}
		
		String[] toks = version.split("\\.");		
		if (toks.length >= 3) {
			try {
				
				StringBuilder b = new StringBuilder();
				
				int len = toks[0].length();
				if(len == 1) {
					b.append("00");
				} else if (len == 2) {
					b.append("0");
				}
				b.append(toks[0]);
				
				len = toks[1].length();
				if(len == 1) {
					b.append("00");
				} else if (len == 2) {
					b.append("0");
				}
				b.append(toks[1]);
				
				for(int i = 0; i < toks[2].length(); i++) {
					char c = toks[2].charAt(i);
					if(Character.isDigit(c)) {
						if(i == 0) {
							b.append('.');
						}
						b.append(c);
					} else {
						break;
					}
				}	
				
				cassandraVersion = Double.valueOf(b.toString());
				return cassandraVersion;
				
			} catch (Exception e) {
			}
		}

		throw new IllegalStateException("Can't parse Cassandra release version string: \"" + version + "\"");
	
	}

	/**
	 * @param jobName
	 * @param snapshotName
	 * @return The LongJob instance.
	 */
	public LongJob consRestoreFromSnapshotJob(String jobID, String jobName,
			String snapshotName) {
		final String sn = snapshotName;

		LongJob job = new LongJob(jobID, jobName) {

			@Override
			protected void doJob() throws Exception {
				if (!isLocal) {
					throw new IllegalStateException(
							"Cassandra-server and Doradus-server must be hosted on the same machine.");
				}
				if (!isNormal()) {
					throw new IllegalStateException(
							"Cassandra-server is not in NORMAL operation mode. Try later, please.");
				}
				
				int vcode = (getVersionNumber() < 1001.0) ? 0 : 1;
				String srcName1 = vcode==0 ? "keyspace" : "column_family";
				String srcName2 = vcode==0 ? "keyspaces" : "column_families";

				logger.info("Starting the restoreFromSnapshot: " + sn);

				progress = "Searching for " + srcName2 + " which have this snapshot. ";
				logger.info(progress);
				String[] dataDirs = getDataFileLocations();
				Map<File, File> map = getDataMap(vcode, dataDirs, sn);

				if (map.isEmpty()) {
					logger.info("No one " + srcName1 + " has snapshot \"" + sn + "\".");
					throw new IllegalArgumentException(
							"No one " + srcName1 + " has snapshot \"" + sn + "\".");
				}

				String commitlogDir = getCommitLogLocation();

				try {
					StringBuilder errStr = new StringBuilder();
					progress = "Stopping the Cassandra server ...";
					logger.info(progress);
					if (execDbtool("STOP", errStr) != 0) {
						logger.info("Can't stop Cassandra server.");
						throw new RuntimeException(
								"Can't stop Cassandra server:\n"
										+ errStr.toString());
					}

					progress = "Clearing the commitlog directory ...";
					logger.info(progress);
					clearDirectory(commitlogDir);

					progress = "Clearing and restoring the data directories ...";
					logger.info(progress);

					int sz = map.size();
					int i = 0;
					String pfx;
					for (File ks : map.keySet()) {
						pfx = "[" + (++i) + "/" + sz + "] ";
						progress = pfx + "Clearing directory: " + ks.getAbsolutePath();
						logger.info(progress);
						clearDirectory(ks, null);
						
						File snDir = map.get(ks);
						progress = pfx + "Restoring from:     " + snDir.getAbsolutePath();
						logger.info(progress);
						copyFiles(snDir, ks);
					}

					errStr = new StringBuilder();
					progress = "Starting the Cassandra server ...";
					logger.info(progress);
					if (execDbtool("START", errStr) != 0) {
						logger.info("Can't start Cassandra server.");
						throw new RuntimeException(
								"Can't start Cassandra server:\n"
										+ errStr.toString());
					}

					logger.info("The restoreFromSnapshot succeeded.");
				} finally {
					resetConnection();
				}
			}
		};
		return job;
	}
	
	/**
	 * @return 
	 */
	public String getReleaseVersion() {
		try {
			return (String) getConnection().getAttribute(storageServiceName,
					"ReleaseVersion");
		} catch (Exception ex) {
			throwException("getReleaseVersion failed.", ex);
			return null; /* unreachable */
		}
	}


	/**
	 * @return The operation mode of the Cassandra-node: { NORMAL, CLIENT,
	 *         JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED }.
	 */
	public String getOperationMode() {
		try {
			return (String) getConnection().getAttribute(storageServiceName,
					"OperationMode");
		} catch (Exception ex) {
			throwException("getOperationMode failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The pathname of commit log directory of the Cassandra-node.
	 */
	public String getCommitLogLocation() {
		try {
			return (String) getConnection().getAttribute(storageServiceName,
					"CommitLogLocation");
		} catch (Exception ex) {
			throwException("getCommitLogLocation failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The pathname list of data files directories of the
	 *         Cassandra-node.
	 */
	public String[] getDataFileLocations() {
		try {
			return (String[]) getConnection().getAttribute(storageServiceName,
					"AllDataFileLocations");
		} catch (Exception ex) {
			throwException("getDataFileLocations failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The pathname of saved caches directory of the Cassandra-node.
	 */
	public String getSavedCachesLocation() {
		try {
			return (String) getConnection().getAttribute(storageServiceName,
					"SavedCachesLocation");
		} catch (Exception ex) {
			throwException("getSavedCachesLocation failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The ip-address list of live nodes of the Cassandra-cluster.
	 */
	@SuppressWarnings("rawtypes")
	public List getLiveNodes() {
		try {
			return (List) getConnection().getAttribute(storageServiceName,
					"LiveNodes");
		} catch (Exception ex) {
			throwException("getLiveNodes failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The ip-address list of leaving nodes of the Cassandra-cluster.
	 */
	@SuppressWarnings("rawtypes")
	public List getLeavingNodes() {
		try {
			return (List) getConnection().getAttribute(storageServiceName,
					"LeavingNodes");
		} catch (Exception ex) {
			throwException("getLeavingNodes failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The ip-address list of moving nodes of the Cassandra-cluster.
	 */
	@SuppressWarnings("rawtypes")
	public List getMovingNodes() {
		try {
			return (List) getConnection().getAttribute(storageServiceName,
					"MovingNodes");
		} catch (Exception ex) {
			throwException("getMovingNodes failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The ip-address list of joining nodes of the Cassandra-cluster.
	 */
	@SuppressWarnings("rawtypes")
	public List getJoiningNodes() {
		try {
			return (List) getConnection().getAttribute(storageServiceName,
					"JoiningNodes");
		} catch (Exception ex) {
			throwException("getJoiningNodes failed.", ex);
			return null; /* unreachable */
		}
	}

	/**
	 * @return The ip-address list of unreachable nodes of the
	 *         Cassandra-cluster.
	 */
	@SuppressWarnings("rawtypes")
	public List getUnreachableNodes() {
		try {
			return (List) getConnection().getAttribute(storageServiceName,
					"UnreachableNodes");
		} catch (Exception ex) {
			throwException("getUnreachableNodes failed.", ex);
			return null; /* unreachable */
		}
	}

	@SuppressWarnings("rawtypes")
	public int getNodesCount() {
		try {
			int size = 0;
			List nodes;

			nodes = (List) getConnection().getAttribute(storageServiceName,
					"UnreachableNodes");
			if (nodes != null)
				size += nodes.size();
			nodes = (List) getConnection().getAttribute(storageServiceName,
					"JoiningNodes");
			if (nodes != null)
				size += nodes.size();
			nodes = (List) getConnection().getAttribute(storageServiceName,
					"MovingNodes");
			if (nodes != null)
				size += nodes.size();
			nodes = (List) getConnection().getAttribute(storageServiceName,
					"LeavingNodes");
			if (nodes != null)
				size += nodes.size();
			nodes = (List) getConnection().getAttribute(storageServiceName,
					"LiveNodes");
			if (nodes != null)
				size += nodes.size();
			return size;

		} catch (Exception ex) {
			throwException("getNodesCount failed.", ex);
			return 0; /* unreachable */
		}
	}

	/**
	 * @return The token string of the Cassandra-node.
	 */
	public String getToken() {
		try {
			return (String) getConnection().getAttribute(storageServiceName,
					"Token");
		} catch (Exception ex) {
			throwException("getToken failed.", ex);
			return null; /* unreachable */
		}
	}

//	/**
//	 * @return a String[] of names of key-spaces which exist on local(!)
//	 *         Cassandra-node.
//	 */
//	public String[] getKeySpaceList() {
//		if (!isLocal) {
//			throw new IllegalStateException(
//					"Cassandra-node and Doradus-server must be hosted on the same machine.");
//		}
//
//		String[] dataDirs = getDataFileLocations();
//		Map<File, File[]> map = getDataMap_1_0(dataDirs);
//		String[] keyspaces = new String[map.size()];
//
//		int i = 0;
//		for (File ks : map.keySet()) {
//			keyspaces[i++] = ks.getName();
//		}
//
//		return keyspaces;
//	}

	/**
	 * @return a String[] of names of snapshots which exist on local(!)
	 *         Cassandra-node.
	 */
	public String[] getSnapshotList() {
		if (!isLocal) {
			throw new IllegalStateException(
					"Cassandra-node and Doradus-server must be hosted on the same machine.");
		}

		String[] dataDirs = getDataFileLocations();
		int vcode = (getVersionNumber() < 1001.0) ? 0 : 1;
		Map<File, File[]> map = (vcode == 0) ? getDataMap_1_0(dataDirs) : getDataMap_1_1(dataDirs);
		Set<String> snapshots = new HashSet<String>();

		for (File ks : map.keySet()) {
			File[] sn = map.get(ks);

			if (sn != null) {
				for (int i = 0; i < sn.length; i++) {
					snapshots.add(sn[i].getName());
				}
			}
		}

		return snapshots.toArray(new String[snapshots.size()]);
	}

	/**
	 * @return The URL of MBeanServer of the node..
	 */
	public JMXServiceURL getJMXServiceURL() {
		return jmxServiceURL;
	}

	/**
	 * @return The connection to MBeanServer of the node.
	 */
	public MBeanServerConnection getJMXServiceConnection() {
		return getConnection();
	}

	/**
	 * @param snapshotName
	 * @param keySpaceList
	 *            ....Not null !
	 * @throws StorageManagementException
	 */
	public void clearSnapshot(String snapshotName, String[] keySpaceList) {
		logger.info("Starting the clearSnapshot: " + snapshotName);

		try {
			String operationName = "clearSnapshot";
			Object[] arguments = new Object[] { snapshotName, keySpaceList };
			String[] signature = new String[] { String.class.getName(),
					String[].class.getName() };

			getConnection().invoke(storageServiceName, operationName,
					arguments, signature);
			logger.info("The clearSnapshot succeeded. ");

		} catch (Exception ex) {
			throwException("clearSnapshot failed.", ex);
			return; /* unreachable */
		} finally {
			lockedMessages.remove("getDataMap");
		}
	}

	/**
	 * @param snapshotName
	 * @param keySpaceList
	 *            ....Not null !
	 */
	public void takeSnapshot(String snapshotName, String[] keySpaceList) {
		logger.info("Starting the takeSnapshot: " + snapshotName);

		try {
			String operationName = "takeSnapshot";
			Object[] arguments = new Object[] { snapshotName, keySpaceList };
			String[] signature = new String[] { String.class.getName(),
					String[].class.getName() };

			getConnection().invoke(storageServiceName, operationName,
					arguments, signature);
			logger.info("The takeSnapshot succeeded. ");

		} catch (Exception ex) {

			String msg = ex.getMessage();
			if (msg != null && msg.indexOf("already") > 0
					&& msg.indexOf("exists") > 0) {
				throw new IllegalStateException(msg);
			}

			throwException("takeSnapshot failed.", ex);
			return; /* unreachable */
			
		} finally {
			lockedMessages.remove("getDataMap");
		}
	}

	// ////////////////////////
	private MBeanServerConnection getConnection() {
		if (connection == null) {
			try {
				setConnection();
			} catch (Exception ex) {
				throwException("Can't create connection to MBean server", ex);
			}
		}
		return connection;
	}

	private OperatingSystemMXBean getOperatingSystemMXBeanProxy() {
		if (osServiceProxy == null) {
			try {
				setConnection();
			} catch (Exception ex) {
				throwException("Can't create proxy of OperatingSystemMXBean",
						ex);
			}
		}
		return osServiceProxy;
	}

	private RuntimeMXBean getRuntimeMXBeanProxy() {
		if (runtimeServiceProxy == null) {
			try {
				setConnection();
			} catch (Exception ex) {
				throwException("Can't create proxy of RuntimeMXBean", ex);
			}
		}
		return runtimeServiceProxy;
	}

	private synchronized void resetConnection() {
		connection = null;
		runtimeServiceProxy = null;
		osServiceProxy = null;
	}

	private synchronized void setConnection() throws IOException {
		if (connection == null) {
			jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://"
					+ host + ":" + port + "/jmxrmi");
			JMXConnector jmxc = JMXConnectorFactory
					.connect(jmxServiceURL, null);
			connection = jmxc.getMBeanServerConnection();

			runtimeServiceProxy = (RuntimeMXBean) JMX.newMXBeanProxy(
					connection, runtimeServiceName, RuntimeMXBean.class);

			osServiceProxy = (OperatingSystemMXBean) JMX.newMXBeanProxy(
					connection, osServiceName, OperatingSystemMXBean.class);

			logger.info("MBean server connected.");
			
			lockedMessages.clear();
		}
	}

	private void throwException(String prefix, Exception ex) {
		String logmsg = consLogMsg(prefix, ex);
		String errmsg = consErrMsg(prefix, ex);
		boolean writeToLog = (connection != null);

		if (java.net.ConnectException.class.isInstance(ex)) {
			resetConnection();
			if (writeToLog)
				logger.error(logmsg);
			throw new RuntimeException(errmsg);

		} else if (java.rmi.ConnectException.class.isInstance(ex)) {
			resetConnection();
			if (writeToLog)
				logger.error(logmsg);
			throw new RuntimeException(errmsg);

		} else {
			Throwable cause = ex.getCause();
			while (cause != null) {
				if (java.net.ConnectException.class.isInstance(cause)) {
					resetConnection();
					if (writeToLog)
						logger.error(logmsg);
					throw new RuntimeException(errmsg + "\nCause: "
							+ cause.getClass().getName() + ":\n"
							+ cause.getMessage());
				} else if (java.rmi.ConnectException.class.isInstance(cause)) {
					resetConnection();
					if (writeToLog)
						logger.error(logmsg);
					throw new RuntimeException(errmsg + "\nCause: "
							+ cause.getClass().getName() + ":\n"
							+ cause.getMessage());
				} else {
					cause = cause.getCause();
				}
			}

			if (writeToLog)
				logger.error(logmsg);
			throw new RuntimeException(prefix, ex);
		}

	}

	private String consErrMsg(String prefix, Exception ex) {
		return getClass().getSimpleName() + ": " + consLogMsg(prefix, ex);
	}

	private String consLogMsg(String prefix, Exception ex) {
		String msg = ex.getClass().getName() + ":\n" + ex.getMessage();
		if (prefix != null && prefix.length() > 0) {
			return prefix + ": " + msg;
		}
		return msg;
	}

	private void clearDirectory(String path) {
		clearDirectory(new File(path), null);
	}

	private void clearDirectory(File dir, String ext) {
		File[] files = dir.listFiles();

		for (int i = 0; i < files.length; i++) {
			File file = files[i];

			if (file.isFile() && (ext == null || file.getName().endsWith(ext))) {
				if (DEBUG) {
					System.out.println("Deleting " + file);
				} else {
					file.delete();
				}
			}
		}
	}

	private void copyFiles(File srcDir, File dstDir) throws IOException {
		File[] src = srcDir.listFiles();
		for (int i = 0; i < src.length; i++) {
			if (src[i].isFile()) {
//				copyFile(src[i], new File(dstDir, src[i].getName()));
				createHardLinkWithExec(src[i], new File(dstDir, src[i].getName()));
			}
		}
	}

//	private void copyFile(File src, File dst) throws IOException {
//		if (DEBUG) {
//			System.out.println("Copying \"" + src + "\" to  \"" + dst + "\"");
//			return;
//		}
//
//		InputStream in = new FileInputStream(src);
//		OutputStream out = new FileOutputStream(dst);
//
//		try {
//			byte[] buf = new byte[1024];
//			int len;
//			while ((len = in.read(buf)) > 0) {
//				out.write(buf, 0, len);
//			}
//		} finally {
//			in.close();
//			out.close();
//		}
//	}
	
	private void createHardLinkWithExec(File src, File dst) throws IOException {
		if (DEBUG) {
			System.out.println("Creating hard link: src = \"" + src + "\"  dst = \"" + dst + "\"");
			return;
		}
    	
		ProcessBuilder pb;
		if (osversion >= 6.0f) {
			pb = new ProcessBuilder("cmd", "/c", "mklink", "/H",
					dst.getAbsolutePath(),
					src.getAbsolutePath());
		} else {
			pb = new ProcessBuilder("fsutil", "hardlink", "create",
					dst.getAbsolutePath(),
					src.getAbsolutePath());
		}

		try {
			exec(pb);
		} catch (IOException ex) {
			String msg = "Unable to create hard link.  This probably means your data directory path is too long.  Exception follows: ";
			logger.error(msg, ex);
			throw ex;
		}
	}

	private void exec(ProcessBuilder pb) throws IOException {
		Process p = pb.start();
		try {
			int errCode = p.waitFor();
			if (errCode != 0) {
				BufferedReader in = new BufferedReader(new InputStreamReader(
						p.getInputStream()));
				BufferedReader err = new BufferedReader(new InputStreamReader(
						p.getErrorStream()));
				
				StringBuilder sb = new StringBuilder();
				String str;
				while ((str = in.readLine()) != null)
					sb.append(str).append(System.getProperty("line.separator"));
				while ((str = err.readLine()) != null)
					sb.append(str).append(System.getProperty("line.separator"));
				
				throw new IOException("Exception while executing the command: "
						+ join(pb.command(), " ") +
						". Error Code: " + errCode
						+ ". Command output: " + sb.toString());
			}
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		} finally {
			if (p != null) {
				p.getErrorStream().close();
				p.getInputStream().close();
				p.getOutputStream().close();
				p.destroy();
			}
		}
	}
	
	private String join(List<String> list, String sep) {
		StringBuilder b = new StringBuilder();
		for(String item : list) {
			b.append(item + sep);
		}
		return b.toString();
	}

	private int execDbtool(String commandName, StringBuilder errStr)
			throws IOException {
		ServerConfig c = ServerConfig.getInstance();

		String[] command;
		if (c.dbhome != null) {
			logger.debug("dbhome: " + c.dbhome);
			command = new String[] { dbtoolName, "-home", c.dbhome, commandName };
		} else {
			command = new String[] { dbtoolName, commandName };
		}

		return exec(command, dbtoolWorkDir, errStr);
	}

	private int exec(String[] command, String workingDir, StringBuilder errStr)
			throws IOException {
		// String[] command = {"CMD", "/C", "dir"};

		String cmdStr = Arrays.toString(command);
		logger.info("Executing " + cmdStr + ", workingDir: " + workingDir);

		String cmd = "[" + command[0] + "]";
		ProcessBuilder pb = new ProcessBuilder(command);
		if (workingDir != null) {
			pb.directory(new File(workingDir));
		}
		pb.redirectErrorStream(true); // merge sdterr with stdout
		Process process = pb.start();

		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);

		String line;
		while ((line = br.readLine()) != null) {
			if (line != null && !line.isEmpty()) {
				logger.info(cmd + ": " + line);
				if (errStr != null && line.startsWith("Error")
						|| line.indexOf("Failed") >= 0
						|| line.indexOf("Unable") >= 0) {
					errStr.append(line + "\n");
				}
			}
		}

		// Wait to get exit value
		try {
			int exitValue = process.waitFor();
			logger.info(cmd + " terminated with status: " + exitValue);
			return exitValue;

		} catch (InterruptedException e) {
			logger.warn("The waiting for " + cmd + " termination interrupted.");
			return 1;
		}
	}

	// keyspace-dir -> snapshot-dir-list
	private Map<File, File[]> getDataMap_1_0(String[] dataDirs) {
		Map<File, File[]> map = new HashMap<File, File[]>();

		boolean doLog = false;
		if(!lockedMessages.contains("getDataMap")) {
			lockedMessages.add("getDataMap");
			doLog = true;
		}

		for (int i = 0; i < dataDirs.length; i++) {
			File dir = new File(dataDirs[i]);
			if (!dir.canRead()) {
				if(doLog) {
					logger.warn("Can't read: " + dir);
				}
				continue;
			}
			
			File[] ksList = dir.listFiles();
			for (int j = 0; j < ksList.length; j++) {

				if (ksList[j].isDirectory()) {
					ArrayList<File> snapshots = new ArrayList<File>();

					File snDir = new File(ksList[j], SNAPSHOTS_DIR_NAME);
					if (snDir.isDirectory()) {

						File[] snList = snDir.listFiles();

						for (int k = 0; k < snList.length; k++) {
							if (snList[k].isDirectory()) {
								snapshots.add(snList[k]);
							}
						}
					}

					map.put(ksList[j],
							snapshots.toArray(new File[snapshots.size()]));
				}
			}

		}
		
//		if(doLog) {
//			logger.debug("keyspacesCount=" + map.size());
//			
//			for(File f : map.keySet()) {
//				File[] snaps = map.get(f);
//				logger.debug("keyspace: " + f.getAbsolutePath() + ",  snapshotsCount=" + snaps.length);
//	
//				for(int i = 0; i < snaps.length; i++) {
//					logger.debug("snaps[" + i + "]: " + snaps[i]);
//				}
//			}
//		}
		
		return map;
	}


	// family-dir -> snapshot-dir-list
	private Map<File, File[]> getDataMap_1_1(String[] dataDirs) {
		Map<File, File[]> map = new HashMap<File, File[]>();

		boolean doLog = false;
		if(!lockedMessages.contains("getDataMap")) {
			lockedMessages.add("getDataMap");
			doLog = true;
		}

		for (int i = 0; i < dataDirs.length; i++) {
			File dir = new File(dataDirs[i]);
			if (!dir.canRead()) {
				if(doLog) {
					logger.warn("Can't read: " + dir);
				}
				continue;
			}
			
			File[] ksList = dir.listFiles();
			for (int j = 0; j < ksList.length; j++) {

				if (ksList[j].isDirectory()) {
					File[] familyList = ksList[j].listFiles();
					
					for(int n = 0; n < familyList.length; n++) {
						if(familyList[n].isDirectory()) {
							ArrayList<File> snapshots = new ArrayList<File>();

							File snDir = new File(familyList[n], SNAPSHOTS_DIR_NAME);
							if (snDir.isDirectory()) {

								File[] snList = snDir.listFiles();

								for (int k = 0; k < snList.length; k++) {
									if (snList[k].isDirectory()) {
										snapshots.add(snList[k]);
									}
								}
							}

							map.put(familyList[n],
									snapshots.toArray(new File[snapshots.size()]));
							
						}						
					}					
				}
			}
		}
		
//		if(doLog) {
//			logger.debug("familiesCount=" + map.size());
//			
//			for(File f : map.keySet()) {
//				File[] snaps = map.get(f);
//				logger.debug("family: " + f.getAbsolutePath() + ",  snapshotsCount=" + snaps.length);
//	
//				for(int i = 0; i < snaps.length; i++) {
//					logger.debug("snaps[" + i + "]: " + snaps[i]);
//				}
//			}
//		}
		
		return map;
	}

	// keyspace-dir -> snapshot-dir
	private Map<File, File> getDataMap(int vcode, String[] dataDirs, String snapshotName) {
		Map<File, File> map = new HashMap<File, File>();

		Map<File, File[]> src = vcode==0 ? getDataMap_1_0(dataDirs) : getDataMap_1_1(dataDirs);
		for (File ks : src.keySet()) {
			File[] sn = src.get(ks);

			if (sn != null) {
				for (int i = 0; i < sn.length; i++) {
					if (snapshotName.equals(sn[i].getName())) {
						map.put(ks, sn[i]);
						break;
					}
				}
			}
		}

		return map;
	}

	private boolean isLocalHost() {
		if (host == null || "".equals(host)) {
			return true;
		}

		try {
			boolean[] local = new boolean[1];
			this.host = ServerMonitor.extractValidHostname(host, local);
			logger.debug("Database hostname: " + this.host + " (local=" + local[0] + ").");
			
			return local[0];
		} catch (Exception ex) {
			logger.warn(ex.getMessage());
			return false;
		}
	}

	private void setDbtoolProps() {
		ServerConfig c = ServerConfig.getInstance();
		if (c.dbtool != null) {
			ClassLoader loader = ServerConfig.class.getClassLoader();
			URL url = loader.getResource(c.dbtool);

			if (url != null) {
				File f = new File(url.getFile());
				dbtoolName = f.getName();
				dbtoolWorkDir = f.getParent();

				logger.info("dbtoolName:    " + dbtoolName);
				logger.info("dbtoolWorkDir: " + dbtoolWorkDir);
			} else {
				logger.warn("\"" + c.dbtool + "\" file could not be found.");
			}
		} else {
			logger.warn("'dbtool' value is not defined in server configuration.");
		}
	}

	private String host;
	private int port;
	private boolean isLocal;
	private JMXServiceURL jmxServiceURL;
	private MBeanServerConnection connection;
	private ObjectName storageServiceName;
	private ObjectName runtimeServiceName;
	private RuntimeMXBean runtimeServiceProxy;
	private ObjectName osServiceName;
	private OperatingSystemMXBean osServiceProxy;
	private String dbtoolName;
	private String dbtoolWorkDir;	
	private Set<String> lockedMessages;
}
