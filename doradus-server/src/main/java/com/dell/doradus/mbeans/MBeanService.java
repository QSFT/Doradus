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

import com.dell.doradus.service.Service;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.rest.RESTService.RequestCallback;

/**
 * Wraps the {@link MBeanProvider} class as a {@link Service} so it can be optionally
 * initialized and started. When started, this service registers a callback with the
 * {@link RESTService} so that connections and requests are monitored.
 */
final public class MBeanService extends Service {
    // Singleton class only
    private static final MBeanService INSTANCE = new MBeanService();
    private MBeanService() { }

    // RESTService.Requestcallback implementation used to monitor requests. Registered
    // with the RESTService only if the MBeanService is started.
    private static class ConnectionMonitor implements RequestCallback {
        @Override
        public void onConnectionOpened() {
            if (INSTANCE.getState().isRunning()) {
                MBeanProvider.getServerMonitor().onConnectionOpened();
            }
        }

        @Override
        public void onConnectionClosed() {
            if (INSTANCE.getState().isRunning()) {
                MBeanProvider.getServerMonitor().onConnectionClosed();
            }
        }

        @Override
        public void onNewRequest() {
            if (INSTANCE.getState().isRunning()) {
                MBeanProvider.getServerMonitor().onNewRequest();
            }
        }

        @Override
        public void onRequestSucceeded(long startTimeNanos) {
            if (INSTANCE.getState().isRunning()) {
                MBeanProvider.getServerMonitor().onRequestSucceeded(startTimeNanos);
            }
        }

        @Override
        public void onRequestRejected(String reason) {
            if (INSTANCE.getState().isRunning()) {
                MBeanProvider.getServerMonitor().onRequestRejected(reason);
            }
        }

        @Override
        public void onRequestFailed(Throwable e) {
            if (INSTANCE.getState().isRunning()) {
                MBeanProvider.getServerMonitor().onRequestFailed(e);
            }
        }
    }   // class ConnectionMonitor
    
    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static MBeanService instance() {
        return INSTANCE;
    }   // instance
    
    //----- Inherited Service methods
    
    @Override
    public void initService() {
        MBeanProvider.populateMBeanServer();
    }   // initService

    @Override
    public void startService() {
        // Monitor requests only when the MBeanService has been started.
        RESTService.instance().registerRequestCallback(new ConnectionMonitor());
    }   // startService

    @Override
    public void stopService() {
        MBeanProvider.unpopulateMBeanServer();
    }   // stopService

}   // class MBeanService
