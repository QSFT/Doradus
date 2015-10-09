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

package com.dell.doradus.service;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.DoradusServer;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.service.db.DBManagerService;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DBService;

/**
 * Abstract root class for Doradus services. Services must provide a singleton object,
 * accessible via a static void no-parameter method called <code>instance()</code>, through
 * which its services are accessed.
 * <p>
 * Each service has a {@link State} that is set as the service is initialized and started
 * by the {@link DoradusServer} via the following methods:
 * <ul>
 * <li>{@link #initialize()}: This method is called once for each service before any
 *     services are started. The service checks configuration options and perform other
 *     one-time initialization, throwing an error if something is misconfigured. This
 *     method causes the service's state to transition from {@link State#INACTIVE} to
 *     {@link State#INITIALIZED}.</li>
 *     
 * <li>{@link #start()}: This method is called once for each service after all services
 *     have been initialized. When this method is called, the service's state is first set
 *     set to {@link State#STARTING}. The service performs startup code in an asynchronous
 *     thread. When startup is complete, the state becomes {@link State#RUNNING}.</li>
 *     
 * <li>{@link #stop()}: This method is called once when the Doradus Server receives a
 *     shutdown request. It allows the service to gracefully clean-up resources:
 *     close sockets, stop threads, etc. This causes the service's state to transition
 *     to {@link State#STOPPING}.
 * </ul>
 * Concrete classes must implement the {@link #initService()}, {@link #startService()},
 * and {@link #stopService()} methods to provide its details. Any service can wait on
 * another service to reach running state or even its own methods by calling
 * {@link #waitForFullService()}.
 */
public abstract class Service {
    
    /**
     * Represents the state of a Service. The states are considered ordered.
     */
    public enum State {
        /**
         * The service ground state before initialization or after stopping.
         */
        INACTIVE,
        
        /**
         * The service has been successfully initialized.
         */
        INITIALIZED,
        
        /**
         * The service has been started but full services are not yet available.
         */
        STARTING,
        
        /**
         * The service has been started and all services are available.
         */
        RUNNING,
        
        /**
         * The service is in the process of shutting down.
         */
        STOPPING;
        
        /**
         * True if this state is {@link #INITIALIZED} or higher.
         */
        public boolean isInitialized() {
            return this.ordinal() >= INITIALIZED.ordinal();
        }
        
        /**
         * True if this state is {@link #STARTING} or higher.
         */
        public boolean isStarted() {
            return this.ordinal() >= STARTING.ordinal();
        }
        
        /**
         * True if this state is {@link #RUNNING} or higher.
         */
        public boolean isRunning() {
            return this.ordinal() >= RUNNING.ordinal();
        }
        
        /**
         * True if this state is {@link #STOPPING}.
         */
        public boolean isStopping() {
            return this == STOPPING;
        }
    }   // enum State

    // Used by start() to call the service's startService() method asynchronously.
    // Notifies all waiters of this service.
    private class AsyncServiceStarter extends Thread {
        @Override
        public void run() {
            try {
                setState(Service.State.STARTING);
                startService();
                setState(Service.State.RUNNING);
            } catch (Throwable e) {
                m_logger.error("Fatal: Failed to enter running state", e);
                m_logger.error("Stopping process");
                setState(Service.State.STOPPING);
                System.exit(1);
            }
        }
    }   // AsyncServiceStarter
    
    // Private Member variables:
    private State           m_state = State.INACTIVE;
    private final Object    m_stateChangeLock = new Object();
    
    // Direct and inherited parameters, visible to subclasses:
    protected final SortedMap<String, Object> m_serviceParamMap = new TreeMap<>();
    
    // Services can set this to wait after serviceStart() is called before start() returns.
    protected int m_startDelayMillis = 0;
    
    // Protected logger available to concrete services:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    protected Service() {
        Map<String, Object> moduleParamMap = ServerParams.instance().getAllModuleParams(this.getClass().getName());
        if (moduleParamMap != null) {
            m_serviceParamMap.putAll(moduleParamMap);
        }
    }
    
    //----- Public service state methods
    
    /**
     * Return the current state of this service.
     * 
     * @return  The current {@link State} of this service.
     */
    public final State getState() {
        return m_state;
    }   // getState
    
    /**
     * Initialize this service. A warning is logged if the service has already been
     * initialized. This method must be called before {@link #start()}. This method
     * causes the service's state to become {@link State#INITIALIZED}.
     */
    public final void initialize() {
        if (m_state.isInitialized()) {
            m_logger.warn("initialize(): Service is already initialized -- ignoring");
        } else {
            logParams("Initializing with the following service parameters:");
            this.initService();
            setState(State.INITIALIZED);
        }
    }   // initialize
    
    /**
     * Start this service. A warning is logged and this method is ignored if the service
     * has not been initialized. It is ignored if the service has already been started.
     * This method uses an asynchronous thread to call {@link #startService()} and adjust
     * the service's state.
     * 
     * @see #startService()
     */
    public final void start() {
        if (!m_state.isInitialized()) {
            m_logger.warn("start(): Service has not been initialized -- ignoring");
        } else if (!m_state.isStarted()) {
            Thread startThread = new AsyncServiceStarter();
            startThread.start();
            if (m_startDelayMillis > 0) {
                try {
                    startThread.join(m_startDelayMillis);
                } catch (InterruptedException e) { }
            }
        }
    }   // start
    
    /**
     * Stop this service. A warning is logged and this method is ignored if the service
     * has not been started. Otherwise, the service's state becomes {@link State#STOPPING}
     * until it is fully stopped, in which case it becomes {@link State#INACTIVE}.
     */
    public final void stop() {
        if (!m_state.isStarted()) {
            m_logger.warn("stop(): Service has not been started -- ignoring");
        } else {
            setState(State.STOPPING);
            this.stopService();
        }
    }   // stop
    
    /**
     * Wait for this service to reach the running state then return. A RuntimeException
     * is thrown if the service has not been initialized.
     * 
     * @throws RuntimeException  If this service has not been initialized.
     */
    public final void waitForFullService() {
        if (!m_state.isInitialized()) {
            throw new RuntimeException("Service has not been initialized");
        }
        synchronized (m_stateChangeLock) {
            // Loop until state >= RUNNING
            while (!m_state.isRunning()) {
                try {
                    m_stateChangeLock.wait();
                } catch (InterruptedException e) {
                }
            }
            if (m_state.isStopping()) {
                throw new RuntimeException("Service " + this.getClass().getSimpleName() +
                                           " failed before reaching running state");
            }
        }
    }   // waitForFullService
    
    /**
     * Get all parameters configured for this Service. The map's key is a parameter name
     * and its value may be a String, List, or Map.
     *  
     * @return  Map of all parameters configured for this service.
     */
    public Map<String, Object> getAllParams() {
        return m_serviceParamMap;
    }
    
    /**
     * Get the value of the parameter with the given name belonging to this service. If
     * the parameter is not found, null is returned.  
     * 
     * @param paramName Name of parameter to find.
     * @return          Parameter found if found for this service, otherwise null.
     */
    public Object getParam(String paramName) {
        return m_serviceParamMap.get(paramName);
    }
    
    /**
     * Get the value of the parameter with the given name belonging to this service as a
     * String. If the parameter is not found, null is returned.
     * 
     * @param paramName Name of parameter to find.
     * @return          Parameter found as a String if found, otherwise null.
     */
    public String getParamString(String paramName) {
        Object paramValue = getParam(paramName);
        if (paramValue == null) {
            return null;
        }
        return paramValue.toString();
    }
    
    /**
     * Get the value of the parameter with the given name belonging to this service as an
     * integer. If the parameter is not found, the given default value is returned. If the
     * parameter is found but cannot be converted to an integer, an
     * IllegalArgumentException is thrown.
     * 
     * @param paramName     Name of parameter to find.
     * @param defaultValue  Value to return if parameter is not defined.
     * @return              Defined or default value.
     */
    public int getParamInt(String paramName, int defaultValue) {
        Object paramValue = getParam(paramName);
        if (paramValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(paramValue.toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Value for parameter '" + paramName + "' must be an integer: " + paramValue);
        }
    }
    
    /**
     * Get the value of the parameter with the given name belonging to this service as a
     * boolean. If the parameter is not found, false is returned.
     * 
     * @param paramName Name of parameter to find.
     * @return          Parameter value found or false if not found.
     */
    public boolean getParamBoolean(String paramName) {
        Object paramValue = getParam(paramName);
        if (paramValue == null) {
            return false;
        }
        return Boolean.parseBoolean(paramValue.toString());
    }

    /**
     * Get the value of the parameter with the given name belonging to this service as a
     * boolean. If the parameter is not found, defaultValue is returned.
     * 
     * @param paramName Name of parameter to find.
     * @return          Parameter value found or defaultValue if not found.
     */
    public boolean getParamBoolean(String paramName, boolean defaultValue) {
        Object paramValue = getParam(paramName);
        if (paramValue == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(paramValue.toString());
    }
    
    /**
     * Get the value of the given parameter name belonging to this service as a LIst of
     * Strings. If no such parameter name is known, null is returned. If the parameter is
     * defined but is not a list, an IllegalArgumentException is thrown. 
     * 
     * @param paramName     Name of parameter to get value of.
     * @return              Parameter value as a List of Strings or null if unknown.
     */
    @SuppressWarnings("unchecked")
    public List<String> getParamList(String paramName) {
        Object paramValue = getParam(paramName);
        if (paramValue == null) {
            return null;
        }
        if (!(paramValue instanceof List)) {
            throw new IllegalArgumentException("Parameter '" + paramName + "' must be a list: " + paramValue);
        }
        return (List<String>)paramValue;
    }
    
    /**
     * Get the value of the given parameter name as a Map. If no such parameter name is
     * known, null is returned. If the parameter is defined but is not a Map, an
     * IllegalArgumentException is thrown. 
     * 
     * @param paramName     Name of parameter to get value of.
     * @return              Parameter value as a String/Objec Map or null if unknown.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getParamMap(String paramName) {
        Object paramValue = getParam(paramName);
        if (paramValue == null) {
            return null;
        }
        if (!(paramValue instanceof Map)) {
            throw new IllegalArgumentException("Parameter '" + paramName + "' must be a map: " + paramValue);
        }
        return (Map<String, Object>)paramValue;
    }
    
    //----- Protected methods 

    /**
     * This method is called {@link #initialize()} is called. Each service must use this
     * method to perform one-time initialization steps such as checking configuration
     * options. It should throw a RuntimeException if the service cannot be started for
     * any reason. If this method returns without exception, the service's state is set to
     * {@link State#INITIALIZED}.
     */
    protected abstract void initService();
    
    /**
     * This method is called when {@link #start()} is called in an asynchronous thread.
     * The thread first sets the services state to {@link State#STARTING} and then calls
     * startService. The method should return only when the service is fully available,
     * hence it should wait on other services, perform database functions, etc. If a fatal
     * error occurs that prevents the service from starting, startService should throw an
     * exception. The exception will be logged and the server process stopped. If no
     * exception occurs, the service's satte is set to {@link State#RUNNING}.
     * <p>
     * Note that the {@link DoradusServer} does not monitor a service for faults once
     * this method has returned. Hence, each service must monitor and report errors. If an
     * error occurs that is considered fatal, the service should call
     * {@link DoradusServer#shutdown(String[])} to force a server shutdown.
     */
    protected abstract void startService();
    
    /**
     * This method is called when {@link #stop()} is called. The service's state is first
     * set to {@link State#STOPPING}. In this method, the service should gracefully
     * shut down: close sockets, stop threads, etc. When the method returns, the service's
     * state is set to {@link State#INACTIVE}
     */
    protected abstract void stopService();
    
    /**
     * This method is the same as {@link #waitForFullService()} except that it
     * additionally ensures the the {@link DBService} is running and throws a
     * {@link DBNotAvailableException} if it isn't. This can be used by public service
     * methods to throw immediately when the DBService hasn't been initialized, thereby
     * returning a 503 to the caller. But if the DBService is running and this service
     * is still starting-up, it waits until it reaches running state.
     * 
     * @throws DBNotAvailableException   If the DBService is not yet running.
     */
    protected void checkServiceState() {
        State dbServiceState = DBManagerService.instance().getState();
        if (!dbServiceState.isInitialized()) {
            throw new RuntimeException("DBService has not been initialized");
        }
        if (!dbServiceState.isRunning()) {
            throw new DBNotAvailableException("Initial Cassandra connection hasn't been established");
        }
        waitForFullService();
    }   // checkServiceState

    //----- Private methods

    // Set the service's state and log the change. Notify all waiters of state change.
    private void setState(State newState) {
        m_logger.debug("Entering state: {}", newState.toString());
        synchronized (m_stateChangeLock) {
            m_state = newState;
            m_stateChangeLock.notifyAll();
        }
    }   // setState
    
    // Print all configuration parameters to the log. 
    private void logParams(String header) {
        m_logger.debug(header);
        for (String paramName : m_serviceParamMap.keySet()) {
            Object paramValue = m_serviceParamMap.get(paramName);
            logParam(paramName, paramValue, "   ");
        }
    }
    
    @SuppressWarnings("unchecked")
    private void logParam(String paramName, Object paramValue, String indent) {
        if (paramValue instanceof List) {
            m_logger.debug(indent + paramName + " (list):");
            logListParams((List<?>)paramValue, indent+"   ");
        } else if (paramValue instanceof Map) {
            m_logger.debug(indent + paramName + " (map):");
            logMapParams((Map<String, ?>)paramValue, indent+"   ");
        } else {
            m_logger.debug(indent + paramName + ": " + paramValue);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void logListParams(List<?> values, String indent) {
        for (Object value : values) {
            if (value instanceof Map) {
                logMapParams((Map<String, ?>)value, indent);
            } else if (value instanceof List) {
                logListParams((List<?>)value, indent);
            } else {
                m_logger.debug(indent + value.toString());
            }
        }
    }
    
    private void logMapParams(Map<String, ?> values, String indent) {
        for (String subParam : values.keySet()) {
            logParam(subParam, values.get(subParam), indent);
        }
    }
    
}   // class Service
