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

/**
 * Contains classes that provide access to Doradus databases using a Java client. The
 * primary classes a Java app will use are summarized below:
 * <ul>
 * <li>{@link com.dell.doradus.client.Client}: Creates a connection to a Doradus server;
 *     allows applications to be defined, modified, and deleted; and creates sessions to
 *     specific applications.</li>
 * <p>
 * <li>{@link com.dell.doradus.client.ApplicationSession}: Abstract class that represents
 *     a client session for a specific Doradus application. Provides access to the
 *     application's definition and methods common to all Doradus applications.</li>
 * <p>
 * <li>{@link com.dell.doradus.client.OLAPSession}: ApplicationSession implementation for
 *     a Doradus OLAP application. Provides methods specific to the OLAP storage service.</li> 
 * <p>
 * <li>{@link com.dell.doradus.client.SpiderSession}: ApplicationSession implementation
 *     for a Doradus Spider application. Provides methods specific to the Spider storage
 *     service.</li>
 * <p>
 * <li>{@link com.dell.doradus.client.RESTClient}: Creates a connection to a Doradus server and
 *     provides lower-level methods for sending requests and reading responses. Reconnects
 *     if a socket is disconnected during a request. A RESTClient can be created first and
 *     then used to create a Client object.</li>
 * <p>
 * <li>{@link com.dell.doradus.client.RESTConnector}: Allows RESTClient connections to be created
 *     from a list of Doradus host addresses. Provides a round-robin distribution of
 *     addresses and maintains the current status of each address in the list. RESTClients
 *     created by RESTConnector can be used directly or used to create Client objects.</li>  
 * </ul>
 * Other classes in this package are used by the classes above to create connections, hold
 * REST results, etc.
 */
package com.dell.doradus.client;

