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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.DoradusServer;

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
 *     have been initialized. Before this method is called, the service's state is set to
 *     {@link State#STARTING}. The service may become ready for general service calls
 *     right away, in which case its state becomes {@link State#RUNNING}. However, if the
 *     service requires more time to initialize, a caller can wait for the service by
 *     calling {@link #waitForFullService()}.</li>
 *     
 * <li>{@link #stop()}: This method is called once when the Doradus Server receives a
 *     shutdown request. It allows the service to gracefully clean-up resources:
 *     close sockets, stop threads, etc. This causes the service's state to transition
 *     to {@link State#STOPPING} and then back to {@link State#INACTIVE}.
 * </ul>
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
         * True if this state is {@link #RUNNING}.
         */
        public boolean isStopping() {
            return this == RUNNING;
        }
    }   // enum State
    
    // Private Member variables:
    private State           m_state = State.INACTIVE;
    private final Object    m_runningLock = new Object();
    
    // Protected logger available to concrete services:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
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
            m_logger.info("Initializing");
            this.initService();
            m_state = State.INITIALIZED;
        }
    }   // initialize
    
    /**
     * Start this service. A warning is logged and this method is ignored if the service
     * has not been initialized. It is ignored if the service has already been started.
     * This method causes the service's start to become {@link State#STARTING}. When the
     * service is fully available for all API calls, its state becomes
     * {@link State#RUNNING}.
     */
    public final void start() {
        if (!m_state.isInitialized()) {
            m_logger.warn("start(): Service has not been initialized -- ignoring");
        } else if (!m_state.isStarted()) {
            m_logger.info("Starting");
            m_state = State.STARTING;
            this.startService();
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
            m_logger.info("Stopping");
            m_state = State.STOPPING;
            this.stopService();
            m_state = State.INACTIVE;
        }
    }   // stop
    
    /**
     * If this service has been initialized, wait for its state to become
     * {@link State#RUNNING}. If the service has not been initialized, a RuntimeException
     * is immediately thrown. Otherwise, this method blocks until the service has signaled
     * that its full services are available.
     * <p> 
     * When a service is started, it may have to wait for a database connection or other
     * events before it is fully available. This method can be called to block until this
     * occurs. Multiple threads can call this method and all will return when the service
     * is running. However, no deadlock detection is done!
     */
    public final void waitForFullService() {
        if (!m_state.isInitialized()) {
            throw new RuntimeException("Service has not been initialized");
        }
        synchronized (m_runningLock) {
            if (!m_state.isStarted()) {
                m_logger.warn("waitForFullService(): Service state is {}", m_state.toString());
            }
            while (!m_state.isRunning()) {
                try {
                    m_runningLock.wait();
                } catch (InterruptedException e) {
                }
            }
        }
    }   // waitForFullService
    
    //----- Protected methods 

    /**
     * Check that this service's state is {@link State#RUNNING} and if it isn't, throw
     * a RuntimeException with the message "Service is not running". This method can be
     * used as a prerequisite in public API methods to safeguard against calling a service
     * that hasn't been initialized and/or hasn't yet started. API methods that intend to
     * block on delayed service starts should call {@link #waitForFullService()} instead
     * of this method. 
     */
    protected void checkRunning() {
        // Don't synchronize on m_runningLock since we're just peeking.
        if (m_state != State.RUNNING) {
            throw new RuntimeException("Service is not running");
        }
    }   // checkRunning
    
    /**
     * Set this service's state to {@link State#RUNNING} and unblock any threads who
     * called {@link #waitForFullService()}. Each service <b>must</b> call this method
     * after {@link #startService()} is called once its services are fully available.
     */
    protected void setRunning() {
        synchronized (m_runningLock) {
            m_state = State.RUNNING;
            m_runningLock.notifyAll();
        }
    }   // setRunning
    
    /**
     * This method is called {@link #initialize()} is called. Each service must use this
     * method to perform one-time initialization steps such as checking configuration
     * options. It should throw a RuntimeException if the service cannot be started for
     * any reason. If this method returns without exception, the service's state is set to
     * {@link State#INITIALIZED}.
     */
    protected abstract void initService();
    
    /**
     * This method is called when {@link #start()} is called. First, the service's
     * state is set to {@link State#STARTING}. If the service is fully available when this
     * method is called, it should call {@link #setRunning()}, which upgrades its state to
     * {@link State#RUNNING}, and then return. If full availability must be delayed, it
     * must launch an asynchronous thread that eventually calls {@link #setRunning()}.
     * Either way, this call cannot block.
     * <p>
     * Note that the {@link DoradusServer} does not monitor a service for faults once
     * this method has been called. Hence, each service must monitor and report errors.
     * If an error occurs that is considered fatal, the service should call
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
    
}   // class Service
