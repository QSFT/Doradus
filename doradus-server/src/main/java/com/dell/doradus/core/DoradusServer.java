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

package com.dell.doradus.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Set;

import org.eclipse.jgit.api.DescribeCommand;
import org.eclipse.jgit.api.Git;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ConfigurationException;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;

/**
 * The container and entrypoint for the Doradus Server. Contains methods to start the
 * server as a console application or as a service, e.g., via procrun. Initializes
 * all services, watches for all a shutdown signal, then shuts down all services.
 */
public final class DoradusServer {
    // Singleton object:
    private static final DoradusServer INSTANCE = new DoradusServer();

    // True after init() and start() have been called, respectively:
    private boolean m_bInitialized;
    private boolean m_bRunning;
    
    // Logging interface:
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    // Services required in every start (in addition to 1 storage service):
    private static final String[] REQUIRED_SERVICES = new String[] {
        com.dell.doradus.service.db.DBService.class.getName(),
        com.dell.doradus.service.schema.SchemaService.class.getName(),
        com.dell.doradus.service.tenant.TenantService.class.getName()
    };
    
    // List of initialized services:
    private final List<Service> m_initializedServices = new ArrayList<>();

    // List of initialized/started StorageServices:
    private final List<StorageService> m_storageServices = new ArrayList<>();
    
    // List of all started services (the ones we must call "stop" on):
    private final List<Service> m_startedServices = new ArrayList<>();
    
    // System commands supported directly by the DoradusServer:
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[] {
        new RESTCommand("GET /_dump com.dell.doradus.core.TheadDumpCmd", true),
        new RESTCommand("GET /_logs?{params} com.dell.doradus.core.LogDumpCmd", true),
        new RESTCommand("GET /_config com.dell.doradus.core.GetConfigCmd", true),
        
    });

    private static final String VERSION_FILE = "doradus.ver";
    ///// Public methods
    
    /**
     * Get the singleton instance of the DoradusServer. The instance may or may not yet be
     * initialized.
     * 
     * @return  The singleton instance of the DoradusServer.
     */
    public static DoradusServer instance() {
        return INSTANCE;
    }   // instance
    
    /**
     * Start the Doradus Server in stand-alone mode, overriding doradus.yaml file options
     * with the given options. All required services plus default_services and
     * storage_services configured in doradus.yaml are started. The process blocks until a
     * shutdown signal is received via Ctrl-C or until {@link #shutdown(String[])} is
     * called.
     * 
     * @param args  Optional arguments that override doradus.yaml file options. Arguments
     *              should be provided in the form "-option value" where "option" is a
     *              doradus.yaml option name and "value" is the overriding value. For
     *              example: "-restport 1223" sets the REST API listening port to 1223.
     */
    public static void main(String[] args) {
        try {
            instance().initStandAlone(args);
            instance().start();
            instance().waitForShutdown();
        } catch (Throwable e) {
            instance().m_logger.error("Abnormal shutdown", e);
            System.exit(1); // invokes shutdown hooks
        }
    }   // main

    /**
     * Entrypoint method when Doradus is run as a Windows service. Currently, does the
     * same thing as {@link #main(String[])}.
     * 
     * @param args  See {@link #main(String[])} for examples;
     */
    public static void startServer(String[] args) {
        main(args);
    }   // startServer
    
    /**
     * Entrypoint method to embed a Doradus server in an application. The args parameter
     * is the same as {@link #startServer(String[])} and {@link #main(String[])}, which
     * override doradus.yaml file defaults. However, instead of starting all services,
     * only those in the given services parameter plus "required" services are started. At
     * least one storage service must be given, otherwise an exception is thrown. Once all
     * services have been started, this method returns, allowing the application to use
     * the now-running embedded server. When the application is done, it should call
     * {@link #shutDown()} or {@link #stopServer(String[])} to gracefully shut down the
     * server.
     * 
     * @param args      See {@link #main(String[])} for more details.
     * @param services  List of service modules to start. The full package name of each
     *                  must be provided. Example service names are:
     * <pre>
     *      com.dell.doradus.mbeans.MBeanService - Provides JMX interface.
     *      com.dell.doradus.service.db.DBService - Database layer.
     *      com.dell.doradus.service.rest.RESTService - Primary REST API service.
     *      com.dell.doradus.service.schema.SchemaService - Schema definition/access service.
     *      com.dell.doradus.service.taskmanager.TaskManagerService - Provides the background task service.
     *      com.dell.doradus.service.spider.SpiderService - Provides the Spider storage service.
     *      com.dell.doradus.service.olap.OLAPService - Provides the OLAP storage service.
     * </pre>
     */
    public static void startEmbedded(String[] args, String[] services) {
        instance().initEmbedded(args, services);
        instance().start();
    }   // startEmbedded
    
    /**
     * Shutdown the Doradus Server by stopping all services and calling System.exit(). The
     * given args are ignored: they are present for compatibility with Apache's procrun
     * application.
     * 
     * @param args  Not currently used (ignored).
     */
    public static void stopServer(String[] args) {
        instance().stop();
        System.exit(0);
    }   // stopServer
    
    /**
     * Shutdown the Doradus Server by stopping all services. Compared to
     * {@link #stopServer(String[])}, this method does not call System.exit(). 
     */
    public static void shutDown() {
        instance().stop();
    }   // shutDown
    
    /**
     * Get the name of the default storage-service for this server. This is either the
     * only {@link StorageService} that has been started, or if there are more than one,
     * the first one configured in the yaml file.
     * 
     * @return  The name of the default storage-service for this server.
     */
    public String getDefaultStorageService() {
        assert m_storageServices.size() > 0;
        return m_storageServices.get(0).getClass().getSimpleName();
    }   // getDefaultStorageService

    /**
     * Find a registered storage service with the given name. If there is no storage
     * service with the registered name, null is returned. This method can only be called
     * after the DoradusServer has initialized.
     * 
     * @param serviceName   Name of the storage service to find. This is the same as the
     *                      class's "simple" name. For example, if the class name is
     *                      "com.dell.doradus.service.spider.SpiderService", the service
     *                      name is just "SpiderService".
     * @return              Singleton instance of the given service name if it is
     *                      registered, otherwise null.
     */
    public StorageService findStorageService(String serviceName) {
        Utils.require(m_bInitialized, "DoradusService has not yet initialized");
        for (StorageService service : m_storageServices) {
            if (service.getClass().getSimpleName().equals(serviceName)) {
                return service;
            }
        }
        return null;
    }   // findStorageService
    
    /**
     * Get Doradus Version from git repo if it exists; otherwise get it from the local doradus.ver file
     * @return version
     */
    public String getDoradusVersion() {
           Git git;
        String version;
        try {
            //first read from the local git repository
            git = Git.open(new File("../.git"));
            DescribeCommand cmd = git.describe();
            version = cmd.call();
            m_logger.info("Doradus version found from git repo", version);
        } catch (Throwable e) {
            //if not found, reading from local file
            try {
                version = getVersionFromVerFile();
                m_logger.info("Doradus version found from doradus.ver file", version);
            } catch (IOException e1) {
                version = null;
            }
        }
        return version;
    }
    
    ///// Private methods

    // Construction via the instance() method only.
    private DoradusServer() {}

    // Add configured storage_services to the given set. 
    private void addConfiguredStorageServices(Set<String> serviceSet) {
        List<String> ssList = ServerConfig.getInstance().storage_services;
        if (ssList != null) {
            serviceSet.addAll(ssList);
        }
    }   // addConfiguredStorageServices
    
    // Add configured default_services to the given set. 
    private void addDefaultServices(Set<String> serviceSet) {
        List<String> defaultServices = ServerConfig.getInstance().default_services;
        if (defaultServices != null) {
            serviceSet.addAll(defaultServices);
        }
    }   // addDefaultServices
    
    // Hook the JVM shutdown hook so we get notified for Ctrl-C.
    private void hookShutdownEvent() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                instance().stop();
            }
        });
    }   // hookShutdownEvent
    
    // Initialize server configuration and given+required services for embedded running.
    private void initEmbedded(String[] args, String[] services) {
        if (m_bInitialized) {
            m_logger.warn("initEmbedded: Already initialized -- ignoring");
            return;
        }
        m_logger.info("Initializing embedded mode");
        initConfig(args);
        initEmbeddedServices(services);
        RESTService.instance().registerGlobalCommands(REST_RULES);
        m_bInitialized = true;
    }   // initEmbedded
    
    // Initialize services required for embedded start.
    private void initEmbeddedServices(String[] requestedServices) {
        Set<String> serviceSet = new LinkedHashSet<>();
        if (requestedServices != null) {
            serviceSet.addAll(Arrays.asList(requestedServices));
        }
        addRequiredServices(serviceSet);
        initServices(serviceSet);
    }   // initEmbeddedServices
    
    // Initialize server configuration and all services for stand-alone running.
    private void initStandAlone(String[] args) {
        if (m_bInitialized) {
            m_logger.warn("initStandAlone: Already initialized -- ignoring");
            return;
        }
        m_logger.info("Initializing standalone mode");
        initConfig(args);
        initStandaAloneServices();
        RESTService.instance().registerGlobalCommands(REST_RULES);
        m_bInitialized = true;
    }   // initStandAlone
    
    // Initialize services configured+needed for stand-alone operation.
    private void initStandaAloneServices() {
        Set<String> serviceSet = new LinkedHashSet<>();
        addDefaultServices(serviceSet);
        addRequiredServices(serviceSet);
        addConfiguredStorageServices(serviceSet);
        initServices(serviceSet);
    }   // initStandaAloneServices

    // Initialize the ServerConfig module, which loads the doradus.yaml file.
    private void initConfig(String[] args) {
        try {
            ServerConfig.load(args);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Failed to initialize server configuration", e);
        }
    }   // initConfig
    
    // Initialize the given list of services. Register initialized Service and
    // StorageService objects. Throw if a storage service is not requested.
    private void initServices(Set<String> serviceSet) {
        for (String serviceName : serviceSet) {
            Service service = initService(serviceName);
            m_initializedServices.add(service);
            if (service instanceof StorageService) {
                m_storageServices.add((StorageService) service);
            }
        }
        if (m_storageServices.size() == 0) {
            throw new RuntimeException("No storage services were configured");
        }
    }   // initServices
    
    // Initialize the service with the given package name.
    private Service initService(String serviceName) {
        m_logger.debug("Initializing service: " + serviceName);
        try {
            @SuppressWarnings("unchecked")
            Class<Service> serviceClass = (Class<Service>) Class.forName(serviceName);
            Method instanceMethod = serviceClass.getMethod("instance", (Class<?>[])null);
            Service instance = (Service)instanceMethod.invoke(null, (Object[])null);
            instance.initialize();
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error initializing service: " + serviceName, e);
        }
    }   // initService

    // Add required services, if missing, to the given list and return.
    private void addRequiredServices(Set<String> serviceSet) {
        serviceSet.addAll(Arrays.asList(REQUIRED_SERVICES));
    }   // addRequiredServices
    
    // Start the DoradusServer services.
    private void start() {
        if (m_bRunning) {
            m_logger.warn("start: Already started -- ignoring");
            return;
        }
        Locale.setDefault(Locale.ROOT);
        m_logger.info("Doradus Version: {}", getDoradusVersion());
        hookShutdownEvent();
        startServices();
        m_bRunning = true;
    }   // start
    
    // Get Version from local file
    private String getVersionFromVerFile() throws IOException {
        
        //declared in a try-with-resource statement, it will be closed regardless of it completes normally or not
        try (BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/" + VERSION_FILE), "UTF-8"))) {
            return br.readLine();
        }

    }

    // Start all registered services.
    private void startServices() {
        m_logger.info("Starting services: {}", simpleServiceNames(m_initializedServices));
        for (Service service : m_initializedServices) {
            m_logger.debug("Starting service: " + service.getClass().getSimpleName());
            service.start();
            m_startedServices.add(service);
        }
    }   // startServices
    
    // Get simple service names as a comma-separated list. 
    private String simpleServiceNames(Collection<Service> services) {
        StringBuilder buffer = new StringBuilder();
        for (Service service : services) {
            if (buffer.length() > 0) {
                buffer.append(",");
            }
            buffer.append(service.getClass().getSimpleName());
        }
        return buffer.toString();
    }   // simpleServiceNames

    // Stop all registered services.
    private void stopServices() {
        // Stop services in reverse order of starting.
        m_logger.debug("Stopping all services");
        ListIterator<Service> iter = m_startedServices.listIterator(m_startedServices.size());
        while (iter.hasPrevious()) {
            Service service = iter.previous();
            m_logger.debug("Stopping service: " + service.getClass().getSimpleName());
            service.stop();
            iter.remove();
        }
        m_initializedServices.clear();
        m_storageServices.clear();
    }   // stopServices
    
    // Shutdown all services and terminate.
    private void stop() {
        if (m_bRunning) {
            instance().m_logger.info("Doradus Server shutting down");
            stopServices();
            ServerConfig.unload();
            m_bRunning = false;
            m_bInitialized = false;
        }
    }   // stop
    
    // Loop forever without consuming resources.
    private void waitForShutdown() {
        m_logger.info("Main thread waiting for shutdown notice");
        synchronized (this) {
            while (true) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        }
    }   // waitForShutdown

}   // class DoradusServer
