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

import java.io.IOException;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Implements a factory of proxy objects of MBeans/MXBeans which compose 
 * a management interface of <i>Doradus</i>-server instance hosted on local 
 * or specified host.
 */
public class MBeanProxyFactory {

        /** Default JMX-port of <i>Doradus</i>-server. (-9999).*/
    public final int DEFAULT_JMX_PORT = 9999;

    /**
     * Creates new factory connected via default port to MBeanServer on local host.
     * @throws IOException 
     */
    public MBeanProxyFactory() throws IOException {
        this(null, 0);
    }

    /**
     * Creates new factory connected via specified port to MBeanServer on specified host.
     * @param host
     * @param port
     * @throws IOException 
     */
    public MBeanProxyFactory(String host, int port) throws IOException {
            if (port <= 0) {
                port = DEFAULT_JMX_PORT;
            }
            connection = createServerConnection(host, port);
    }

    /** 
     * @return The URL of MBeanServer.
     */
    public JMXServiceURL getJMXServiceURL() {
        return jmxServiceURL;
    }

    /** 
     * @return The connection to MBeanServer.
     */
    public MBeanServerConnection getJMXServiceConnection() {
        return connection;
    }
    
    /**
     * Makes the proxy for ServerMonitorMXBean.
     * @return A ServerMonitorMXBean proxy object.
     * @throws IOException 
     */
    public ServerMonitorMXBean createServerMonitorProxy() throws IOException {
        String beanName = ServerMonitorMXBean.JMX_DOMAIN_NAME + ":type=" + ServerMonitorMXBean.JMX_TYPE_NAME;
        return createMXBeanProxy(beanName, ServerMonitorMXBean.class);
    }

    /**
     * Makes the proxy for StorageManagerMXBean.
     * @return A StorageManagerMXBean object.
     * @throws IOException 
     */
    public StorageManagerMXBean createStorageManagerProxy() throws IOException {
        String beanName = StorageManagerMXBean.JMX_DOMAIN_NAME + ":type=" + StorageManagerMXBean.JMX_TYPE_NAME;
        return createMXBeanProxy(beanName, StorageManagerMXBean.class);
    }
    
    
    /////////////////////////////////

    private MBeanServerConnection createServerConnection(String host, int port) throws IOException {
            jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
            JMXConnector jmxc = JMXConnectorFactory.connect(jmxServiceURL, null);
            return jmxc.getMBeanServerConnection();            
    }
    
    @SuppressWarnings("unused")
	private <T> T createMBeanProxy(String beanName, Class<T> clazz) throws IOException {
        try {
            ObjectName objectName = new ObjectName(beanName);
            return JMX.newMBeanProxy(connection, objectName, clazz);
        } catch (MalformedObjectNameException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private <T> T createMXBeanProxy(String beanName, Class<T> clazz) throws IOException {
        try {
            ObjectName objectName = new ObjectName(beanName);
            return JMX.newMXBeanProxy(connection, objectName, clazz);
        } catch (MalformedObjectNameException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private MBeanServerConnection connection;
    private JMXServiceURL jmxServiceURL;
}
