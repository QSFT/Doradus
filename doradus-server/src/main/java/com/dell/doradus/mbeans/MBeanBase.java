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

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for MBeans/MXBeans of <i>Doradus</i> server. 
 */
public class MBeanBase {
	protected Logger logger = LoggerFactory.getLogger(getClass().getSimpleName());


	/**
	 * @return ObjectName of this bean or null, if bean was not registered.
	 */
	public ObjectName getPublicName() {
		return objectName;
	}

	/**
	 * Registers this bean on platform MBeanServer.
	 */
	public void register() {
		try {
			objectName = consObjectName(domain, type, keysString);
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.registerMBean(this, objectName);
		} catch (Exception e) {
			objectName = null;
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deregisters this bean on platform MBeanServer.
	 */
	public void deregister() {
		if(objectName == null) {
			return;
		}
		
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			if (mbs.isRegistered(objectName)) {
				mbs.unregisterMBean(objectName);
			}
			objectName = null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** 
	 * Constructs the ObjectName of bean. 
	 * 
	 * @param domain
	 *            The domain part in object name of bean. It is a string of
	 *            characters not including the character colon (:), wildcard
	 *            characters asterisk (*), and question mark (?). It is
	 *            recommended that the domain should not contain the string "//"
	 *            also. If the domain is null or empty, it will be replaced by
	 *            the package name of this bean's class.
	 * @param type
	 *            A value of key 'type' in the object name of this bean. If the
	 *            given value is null or empty, it will be replaced by the
	 *            simple name of the bean's class.
	 * @param keysString
	 *            A string that represents a set of additional keys and
	 *            associated values in object name of this bean. Example:
	 *            "key1=value1,key2=value2,...". See java-docs of the
	 *            javax.management.ObjectName class for restrictions. Null value
	 *            of the keysString is interpreted as an empty string.
	 */
	protected ObjectName consObjectName(String domain, String type,
			String keysString) {
		String d = domain != null && !"".equals(domain) ? domain
				: getDefaultDomain();
		String t = type != null && !"".equals(type) ? type : getDefaultType();
		String k = keysString != null && !"".equals(keysString) ? ","
				+ keysString : "";

		try {
			return new ObjectName(d + ":type=" + t + k);
		} catch (MalformedObjectNameException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * @return Default domain.
	 */
	protected String getDefaultDomain() {
		return this.getClass().getPackage().getName();
	}

	/**
	 * @return Default value of key 'type'.
	 */
	protected String getDefaultType() {
		return this.getClass().getSimpleName();
	}

	protected ObjectName objectName;
	protected String domain;
	protected String type;
	protected String keysString;
}
