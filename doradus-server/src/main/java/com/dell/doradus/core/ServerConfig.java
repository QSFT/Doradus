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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import com.dell.doradus.common.ConfigurationException;
import com.dell.doradus.common.Utils;

/**
 * Provides the static <code>load(args)</code> method that
 * loads the <i>Doradus</i>-server configuration file and overrides the loaded settings
 * by command line arguments, if any. If configuration file
 * does not exist or can't be loaded then default settings are used. Provides the resulting
 * configuration as a singleton object (see the <code>getInstance()</code> static method).
 * <p>
 * The default constructor of this class creates instance populated by default settings.
 * </p>
 * <p>
 * Configuration file location is specified by system property "doradus.config". The accepted
 * value is either URL string (ex: "file:///c:/bla/bla/foo.yaml"), or absolute/relative pathname
 * (ex: "c:/bla\bla/foo.yaml" or "..\bla\foo.yaml") , or name of file resource (ex: "conf/foo.yaml").
 * In last case, the file must be located on the classpath, to be loadable by this class's ClassLoader.
 * By default, an attempt to load the "doradus.yaml" resource will be done.
 * </p>
 */
public class ServerConfig {

    private static final Logger logger = LoggerFactory.getLogger(ServerConfig.class.getSimpleName());
    private static ServerConfig config;
    //
    // Constants:
    //
    public final static String DEFAULT_CONFIG_URL = "doradus.yaml";
    public final static String CONFIG_URL_PROPERTY_NAME = "doradus.config";
    public final static String SUN_JAVA_COMMAND = "sun.java.command";
    public final static String DEFAULT_PARAM_OVERRIDE_FILE_PARAM = "param_override_filename";

    // Default database connection values:
    public static final String DEFAULT_DB_HOST = "localhost";
    public static final int DEFAULT_DB_PORT = 9160; // Thrift port, not jmx or gossip
    public static final int DEFAULT_JMX_PORT = 7199; // JMX port, not thrift or gossip
    // Default keyspace parameters:
    public static final String  DEFAULT_KS_NAME = "Doradus";
    // Default values for miscellaneous Doradus server options:
    public static final int DEFAULT_REST_PORT = 1123;
    public static final int DEFAULT_MAX_THREADS = 200;
    public static final int DEFAULT_MIN_THREADS = 10;
    public static final int DEFAULT_IDLE_TIMEOUT= 600000;
    public static final int DEFAULT_TASK_QUEUE = 600;
    public static final String DEFAULT_DBTOOL_NAME = "doradus-cassandra.bat";
    public static final String DEFAULT_AGING_RECHECK = "4 hours";
    public static final boolean DEFAULT_AGGR_SEPARATE_SEARH = false;
    public static final int DEFAULT_MAX_REQUEST_SIZE = 50 * 1024 * 1024;    // 50MB
    // Default DBEntitySequenceOptions values
    public static final int DEFAULT_DBESOPTIONS_ENTITY_BUFFER = 1000;
    public static final int DEFAULT_DBESOPTIONS_LINK_BUFFER = 1000;
    public static final int DEFAULT_DBESOPTIONS_INITIAL_LINK_BUFFER = 10;
    public static final int DEFAULT_DBESOPTIONS_INITIAL_LINK_BUFFER_DIMENSION = 1000;
    public static final int DEFAULT_DBESOPTIONS_INITIAL_SCALAR_BUFFER = 30;
	// Default Thrift timeout and retry values (not advertised in getUsage):
    public static final int DEFAULT_DB_TIMEOUT_MILLIS = 10000;
    public static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 10;
    public static final int DEFAULT_MAX_READ_ATTEMPTS = 3;
    public static final int DEFAULT_RETRY_WAIT_MILLIS = 5000;
    public static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 3;
    public static final int DEFAULT_DB_CONNECT_RETRY_WAIT_MILLIS = 5000;
    public static final int DEFAULT_BATCH_MUTATION_THRESHOLD = 10000;
    public static final int DEFAULT_THRIFT_BUFFER_SIZE_MB = 16;
    // Default modified objects shard value:
    public static final String DEFAULT_DATA_CHECK_SHARD_GRAN = "1 DAY";

    // Map of KeySpace default options
    public Map<String, Object> ks_defaults = new HashMap<>();
    
    // Map of ColumnFamily default options
    public Map<String, Object> cf_defaults = new HashMap<>();
    
    //Doradus WebServer
    public String webserver_class;      

    // Allow package-level construction without load().
    static ServerConfig createInstance() {
        assert config == null;
        config = new ServerConfig();
        return config;
    }
    
    /**
     * WARN: The configuration singleton must be already initialized before to call this method (see: <code>load()</code>).
     * @return The ServerConfig singleton.
     * @exception IllegalStateException If the configuration singleton was not initialized.
     */
    public static ServerConfig getInstance() {
        if(config == null) {
            throw new IllegalStateException("The configuration singleton was not initialized. Call the load() method on app's startup. ");
        }
        return config;
    }

    /**
     * Creates and initializes the ServerConfig singleton.
     * @param args The command line arguments.
     * @return
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    public static ServerConfig load(String[] args) throws ConfigurationException {
    	if(config != null) {
    		logger.warn("Configuration is loaded already. Use ServerConfig.getInstance() method. ");
    		return config;
    	}

        try {
            URL url = getConfigUrl();
            logger.info("Trying to load settings from: " + url);

            InputStream input = null;
            try {
                input = url.openStream();
            } catch (IOException e) {
                throw new AssertionError(e);
            }

            Yaml yaml = new Yaml();
            LinkedHashMap<String, ?> dataColl = (LinkedHashMap<String, ?>)yaml.load(input);
            config = new ServerConfig();
            setParams(dataColl);
                       
            logger.info("Ok. Configuration loaded.");

        } catch (ConfigurationException e) {
            logger.warn(e.getMessage() + " -- Ignoring.");
        } catch (YAMLException e) {
            logger.warn(e.getMessage() + " -- Ignoring.");
        }

        if (config == null) {
            logger.info("Initializing configuration by default settings.");
            config = new ServerConfig();
        }

        try {
            if (args != null && args.length > 0) {
                logger.info("Parsing the command line arguments...");
                parseCommandLineArgs(args);
                logger.info("Ok. Arguments parsed.");
            }
        } catch (ConfigurationException e) {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error. Unable to start server.  See log for stacktrace.");
            throw e;
        }

        //overrides with params from the file
        if (!Utils.isEmpty(config.param_override_filename)) {
            try {
                InputStream overrideFile = new FileInputStream(config.param_override_filename);
                Yaml yaml = new Yaml();
                LinkedHashMap<String, ?> overrideColl = (LinkedHashMap<String, ?>)yaml.load(overrideFile);
                setParams(overrideColl);
            } catch (Exception e) {
                logger.warn(e.getMessage() + " -- Ignoring.");
            }       
        }
        return config;
    }
    
    /**
     * Deletes the the ServerConfig singleton if it has been created.
     * <p>NOTE: Intended for testing/debugging purposes (CR#104652).
     */
    public static void unload() {
    	config = null;
    }


    // Database connection properties
    public List<String> default_services = new ArrayList<>();
    public List<String> storage_services = new ArrayList<>();
    public String dbhost = DEFAULT_DB_HOST;
    public String secondary_dbhost = null;
    public int dbport = DEFAULT_DB_PORT;
    public boolean dbtls = false;
    public List<String> dbtls_cipher_suites = new ArrayList<>();
    public String dbuser;
    public String dbpassword;
    public int jmxport = DEFAULT_JMX_PORT;
    // Doradus keyspace properties
    public boolean multitenant_mode = false;
    public String keyspace = DEFAULT_KS_NAME;
    public boolean disable_default_keyspace = false;
    // Miscellaneous Doradus server options:
    public int restport = DEFAULT_REST_PORT;
    public String restaddr;
    public int maxconns = DEFAULT_MAX_THREADS;
    public int defaultMinThreads = DEFAULT_MIN_THREADS;
    public int maxTaskQueue = DEFAULT_TASK_QUEUE;
    public int defaultIdleTimeout = DEFAULT_IDLE_TIMEOUT;
    public boolean tls = false;
    public List<String> tls_cipher_suites = new ArrayList<>();
    public boolean clientauthentication = false;
    public String keystore;
    public String keystorepassword;
    public String truststore;
    public String truststorepassword;
    public String dbhome = null;
    public String dbtool = DEFAULT_DBTOOL_NAME;
    public String aging_recheck_freq = DEFAULT_AGING_RECHECK;
    public boolean aggr_separate_search = DEFAULT_AGGR_SEPARATE_SEARH;
    public int max_request_size = DEFAULT_MAX_REQUEST_SIZE;
    // DBEntitySequenceOptions properties
    public int dbesoptions_entityBuffer = DEFAULT_DBESOPTIONS_ENTITY_BUFFER;
    public int dbesoptions_linkBuffer = DEFAULT_DBESOPTIONS_LINK_BUFFER;
    public int dbesoptions_initialLinkBuffer = DEFAULT_DBESOPTIONS_INITIAL_LINK_BUFFER;
    public int dbesoptions_initialLinkBufferDimension = DEFAULT_DBESOPTIONS_INITIAL_LINK_BUFFER_DIMENSION;
    public int dbesoptions_initialScalarBuffer = DEFAULT_DBESOPTIONS_INITIAL_SCALAR_BUFFER;

    // Enable left-to-right algorithm
    public boolean l2r_enable = true;
    // Default query page size (used when s=... parameter is missing in _query request
    public int search_default_page_size = 100;

    // Thrift timeout and retry values (see documentation in YAML file):
    public int db_timeout_millis = DEFAULT_DB_TIMEOUT_MILLIS;
    public int primary_host_recheck_millis = 60000;
    public int max_commit_attempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
    public int max_read_attempts = DEFAULT_MAX_READ_ATTEMPTS;
    public int retry_wait_millis = DEFAULT_RETRY_WAIT_MILLIS;
    public int max_reconnect_attempts = DEFAULT_MAX_RECONNECT_ATTEMPTS;
    public int db_connect_retry_wait_millis = DEFAULT_DB_CONNECT_RETRY_WAIT_MILLIS;
    public int batch_mutation_threshold = DEFAULT_BATCH_MUTATION_THRESHOLD;
    public int thrift_buffer_size_mb = DEFAULT_THRIFT_BUFFER_SIZE_MB; 

    //OLAP
    public Map<String, Object> olap_cf_defaults = new HashMap<>();
    public int olap_loaded_segments = 8192;
    public boolean olap_internal_compression = true;
    public int olap_cache_size_mb = 100;
    public int olap_file_cache_size_mb = 0; // turned out to be faster without file cache
    public int olap_query_cache_size_mb = 100;
    public int olap_merge_threads = 0;
    public int olap_compression_threads = 0;
    public int olap_search_threads = 0;
    public int olap_compression_level = -1;
    
    // CQL (true) or Thrift (false) API
    public boolean use_cql = true;
    
    // Use a specific DBService class instead of Thrift or CQL.
    public String dbservice;
    
    // Experimental: use async update statement execution
    public boolean async_updates = false;
    
    //
    // search configuration properties
    //
    
    //override property
    public String param_override_filename;

    /**
     * @return The map of the configuration properties.
     */
    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        Field[] fields = ServerConfig.class.getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            int m = f.getModifiers();

            if (Modifier.isPublic(m) && !Modifier.isStatic(m)) {
                try {
                    map.put(f.getName(), f.get(this));
                } catch (IllegalArgumentException ex) {
                } catch (IllegalAccessException ex) {
                }
            }
        }

        return map;
    }


    //
    // Inspect the classpath to find configuration file
    //
    private static URL getConfigUrl() throws ConfigurationException {
        String spec = System.getProperty(CONFIG_URL_PROPERTY_NAME);

        if (spec == null) {
            spec = DEFAULT_CONFIG_URL;
        }

        URL configUrl = null;
        try {
            configUrl = new URL(spec);
            configUrl.openStream().close(); // catches well-formed but bogus URLs
        } catch (Exception e) {
        	try {
        		File f = new File(spec);
        		if(f.exists()) {
        			configUrl = new URL("file:///" + f.getCanonicalPath());
        		}
        	} catch( Exception ex) {
        	}
        }

        if(configUrl == null) {
            ClassLoader loader = ServerConfig.class.getClassLoader();
            configUrl = loader.getResource(spec);

            if (configUrl == null) {
                throw new ConfigurationException("Can't find file/resource: \"" + spec + "\".");
            }
        }

        return configUrl;
    }

    //
    // Overrides the loaded/default settings by command line arguments.
    //
    private static void parseCommandLineArgs(String[] args) throws ConfigurationException {

        if (args[0].equalsIgnoreCase("-?") || args[0].equalsIgnoreCase("-h")
                || args[0].equalsIgnoreCase("/?") || args[0].equalsIgnoreCase("-help")) {
            System.out.println("See documentation and doradus.yaml and options");
            System.exit(0);
        }

        List<String> unknownArgs = new ArrayList<String>();

        for (int inx = 0; inx < args.length; inx++) {
            String name = args[inx].substring(1);
            if (inx + 1 >= args.length) {
                throw new ConfigurationException("A value is expected after: " + args[inx]);
            }
            String value = args[++inx];
            if (!setParamFromString(name, value)) {
                unknownArgs.add(name);
            }
        }

        if (!unknownArgs.isEmpty()) {
            StringBuilder b = new StringBuilder();
            for (String arg : unknownArgs) {
                b.append("\"" + arg + "\" ");
            }

            throw new ConfigurationException("Couldn't parse argument(s): " + b.toString());
        }
    }
    
    // Set configuration parameters from the given YAML-loaded map.
    private static void setParams(Map<String, ?> map) {
        for (String name : map.keySet()) {
            setParam(name, map.get(name));
        }
    }   // setParams
    
    static void setParam(String name, Object value) {
        try {
            if (value instanceof List) {
                setCollectionParam(name, (List<?>)value);
            } else if (value instanceof String) {
                setParamFromString(name, (String)value);
            } else {
                setScalarParam(name, value);
            }
        } catch (ConfigurationException e) {
            logger.warn(e.getMessage() + " -- Ignoring.");
        }
    }
    // Return false if given name is not a known scalar configuration parameter
    private static boolean setParamFromString(String name, String value) throws ConfigurationException {
        try {
            Field field = config.getClass().getDeclaredField(name);
            String fieldType = field.getType().toString();
            if (fieldType.compareToIgnoreCase("int") == 0) {
                field.set(config, Integer.parseInt(value));
            } else if (fieldType.compareToIgnoreCase("boolean") == 0) {
                field.set(config, Boolean.parseBoolean(value));
            } else if (fieldType.endsWith("List")) {
                setCollectionParam(name, Arrays.asList(value.split(",")));
            } else {
                field.set(config, value);
            }
            return true;
        } catch (SecurityException | NoSuchFieldException | IllegalAccessException e) {
            return false;
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("Couldn't parse parameter: " + value);
        }
    }   // setParamFromString
    
    // Return false if given name is not a known scalar configuration parameter
    private static void setScalarParam(String name, Object value) throws ConfigurationException {
        try {
            Field field = config.getClass().getDeclaredField(name);
            field.set(config, value);
        } catch (SecurityException | NoSuchFieldException | IllegalAccessException e) {
            throw new ConfigurationException("Unknown configuration parameter: " + name);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("Couldn't parse parameter: " + value);
        }
    }   // setScalarParam
    
    // Set a configuration parameter with a Collection type.
    private static void setCollectionParam(String name, List<?> values) throws ConfigurationException {
        try {
            Field field = config.getClass().getDeclaredField(name);
            Class<?> fieldClass = field.getType();
            if (Map.class.isAssignableFrom(fieldClass)) {
                setMapParam(field, values);
            } else if (List.class.isAssignableFrom(fieldClass)) {
                setListParam(field, values);
            } else {
                throw new ConfigurationException("Invalid value type for parameter: " + name);
            }
        } catch (SecurityException | NoSuchFieldException e) {
            throw new ConfigurationException("Unknown configuration parameter: " + name);
        }
    }   // setCollectionParam

    // Set a List<String> configuration parameter
    private static void setListParam(Field field, List<?> values) {
        try {
            // Find method: clear() and call it
            Class<?> fieldClass = field.getType();
            Method clearMethod = fieldClass.getMethod("clear");
            clearMethod.invoke(field.get(config));
            
            // Find List method: add(Object) 
            Method addMethod = fieldClass.getMethod("add", Object.class);
            
            // Add each map key/value to field value
            for (Object subParam : values) {
                if (subParam instanceof String) {
                    addMethod.invoke(field.get(config), subParam);
                } else {
                    throw new RuntimeException("Unexpected parameter type for '" + field.getName() + ":" + subParam);
                }
            }
        } catch (NoSuchMethodException  | IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Error setting configuration parameter '" + field.getName() + "'", e);
        }
    }   // setListParam
    
    // Set a Map<String, Object> configuration parameter
    @SuppressWarnings("unchecked")
    private static void setMapParam(Field field, List<?> values) {
        try {
            // Find Map method: put(Object, Object)
            Class<?> fieldClass = field.getType();
            Method addMethod = fieldClass.getMethod("put", Object.class, Object.class);
            
            // Add each map key/value to field value
            for (Object subParam : values) {
                if (subParam instanceof Map) {
                    Map<String, ?> map = (Map<String, ?>)subParam;
                    for (String key : map.keySet()) {
                        Object value = map.get(key);
                        if (value instanceof List) {
                            // Contains a single element, which is a key/value map
                            value = ((List<?>)value).get(0);
                        }
                        addMethod.invoke(field.get(config), key, value);
                    }
                } else {
                    throw new RuntimeException("Unexpected collection parameter type for '" + field.getName() + ":" + subParam);
                }
            }
        } catch (NoSuchMethodException  | IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Error setting configuration parameter '" + field.getName() + "'", e);
        }
    }   // setMapParam

}   // class ServerConfig

