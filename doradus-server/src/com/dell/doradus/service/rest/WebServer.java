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

package com.dell.doradus.service.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.service.rest.RESTService.RequestCallback;

/**
 * Abstract root class for WebServer implementation
 */
public abstract class WebServer {
	
	/**
	 * initialize WebServer instance with servletClassName
	 * @param servletClassName
	 */
	public abstract void init(String servletClassName);  
	
	/**
	 * start WebServer
	 */
	public abstract void start();
	
	
	/**
	 * stop WebServer
	 */
	public abstract void stop();
	
	/**
	 * register Register the given request callback
	 * @param callback
	 */
	public abstract void registerRequestCallback(RequestCallback callback);
	
	/**
	 * Notify the given request callback when there is a new request
	 */
	public abstract void notifyNewRequest();
	
	/**
	 * Notify the given request callback when request is successful
	 * @param startTimeNanos
	 */
	public abstract void notifyRequestSuccess(long startTimeNanos);
	
	/**
	 * Notify the given request callback when request is rejected with a reason
	 * @param reason
	 */
	public abstract void notifyRequestRejected(String reason);
	
	/**
	 * Notify the given request callback when request is failed with a reason
	 * @param e
	 */
	public abstract void notifyRequestFailed(Throwable e);	
	
	protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
}
