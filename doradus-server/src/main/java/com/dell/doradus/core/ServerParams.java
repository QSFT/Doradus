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
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.dell.doradus.common.ConfigurationException;
import com.dell.doradus.common.Utils;

/**
 * Parses the Doradus configuration yaml file and stores parameters. The default file
 * "doradus.yaml" must be in the class path. A non-default file can be used by setting the
 * environment variable {@value #CONFIG_URL_PROPERTY_NAME}. The {@link #load(String[])}
 * method must be called before {@link #instance()} can be called to get the singleton
 * configuration object.
 */
public class ServerParams {
    // Constants:
    public final static String DEFAULT_CONFIG_URL = "doradus.yaml";
    public final static String CONFIG_URL_PROPERTY_NAME = "doradus.config";

    private static final Logger logger = LoggerFactory.getLogger(ServerParams.class.getSimpleName());
    private static ServerParams INSTANCE;

    // Local members:
    private final Map<String, Object> m_params = new HashMap<>();
    private String[] m_commandLineArgs;
    private boolean m_bWarnedLegacyParam;

    //----- Map to legacy configuration names: delete after transition
    private final static Map<String, String> g_legacyToModuleMap = new HashMap<>();
    
    // Register the given legacy parameter name as owned by the given module name.
    private static void setLegacy(String moduleName, String legacyParamName) {
        String oldValue = g_legacyToModuleMap.put(legacyParamName, moduleName);
        if (oldValue != null) {
            logger.warn("Legacy parameter name used twice: {}", legacyParamName);
        }
    }
    
    // Register the given legacy parameter names as owned by the given module name.
    private static void setLegacy(String moduleName, String... legacyParamNames) {
        for (String legacyParamName : legacyParamNames) {
            setLegacy(moduleName, legacyParamName);
        }
    }

    // Modules and the legacy parameters they own.
    static {
        setLegacy("DoradusServer",
            "default_services",
            "l2r_enable",
            "search_default_page_size",
            "storage_services",
            "aggr_separate_search",
            "dbesoptions_entityBuffer",
            "dbesoptions_initialLinkBuffer",
            "dbesoptions_initialLinkBufferDimension",
            "dbesoptions_initialScalarBuffer",
            "dbesoptions_linkBuffer",
            "param_override_filename"
        );
        
        setLegacy("TenantService",
            "disable_default_keyspace",
            "multitenant_mode"
        );
        
        setLegacy("RESTService",
            "clientauthentication",
            "defaultIdleTimeout",
            "defaultMinThreads",
            "keystore",
            "keystorepassword",
            "maxconns",
            "max_request_size",
            "maxTaskQueue",
            "restaddr",
            "restport",
            "tls",
            "tls_cipher_suites",
            "truststore",
            "truststorepassword",
            "webserver_class"
        );
        
        setLegacy("DBService",
            "async_updates",
            "cf_defaults",
            "db_connect_retry_wait_millis",
            "db_timeout_millis",
            "dbhost",
            "dbpassword",
            "dbport",
            "dbservice",
            "dbtls",
            "dbtls_cipher_suites",
            "dbuser",
            "jmxport",
            "keyspace",
            "ks_defaults",
            "max_commit_attempts",
            "max_read_attempts",
            "max_reconnect_attempts",
            "primary_host_recheck_millis",
            "retry_wait_millis",
            "secondary_dbhost",
            "thrift_buffer_size_mb",
            "use_cql"
        );
        
        setLegacy("OLAPService",
            "olap_cache_size_mb",
            "olap_cf_defaults",
            "olap_compression_level",
            "olap_compression_threads",
            "olap_file_cache_size_mb",
            "olap_internal_compression",
            "olap_loaded_segments",
            "olap_merge_threads",
            "olap_query_cache_size_mb",
            "olap_search_threads"
        );
        
        setLegacy("SpiderService", "batch_mutation_threshold");
        
        setLegacy("CassandraNode",
            "dbhome",
            "dbtool"
        );
    }

    /**
     * Get the configuration singleton. {@link #load(String[])} must be called first.
     *  
     * @return  The {@link ServerParams} singleton.
     * @see:    {@link #load(String[])}
     */
    public static ServerParams instance() {
        if (INSTANCE == null) {
            throw new RuntimeException("The configuration singleton was not initialized. Call the load() first.");
        }
        return INSTANCE;
    }

    /**
     * Creates and initializes the {@link ServerParams} singleton.
     * 
     * @param args  The command line arguments.
     * @return      The singleton object.
     * @throws      ConfigurationException if an error occurs parsing the input YAML file
     *              or command line arguments.
     */
    public static ServerParams load(String[] args) throws ConfigurationException {
        if (INSTANCE != null) {
            logger.warn("Configuration is loaded already. Use ServerParams.getInstance() method. ");
            return INSTANCE;
        }

        INSTANCE = new ServerParams();
        INSTANCE.parseConfigFile(getConfigUrl());
        INSTANCE.parseCommandLineArgs(args);
        INSTANCE.parseOverrideFile();
        INSTANCE.printConfig();
        
        // TODO: Delete the next line when ServerConfig is phased out.
        copyParamsToServerConfig();
        return INSTANCE;
    }

    /**
     * Uninitialize configuration options. Use for shutting down.
     */
    public static void unload() {
        INSTANCE = null;
    }
    
    /**
     * Get the parameters loaded for the given module name, if any. The result will be
     * null if the module name is unknown or no parameters have been loaded. Otherwise,
     * the object returned will be parsed configuration parameter(s), which in most cases
     * can be cast to a Map<String,Object>.
     * 
     * @param   moduleName  Name of module to get parameters for.
     * @return              Module's parameter, usually a map, or null if none.
     */
    public Object getModuleParams(String moduleName) {
        return m_params.get(moduleName);
    }

    /**
     * Get the value of the given parameter name belonging to the given module name. If
     * no such module/parameter name is known, null is returned. Otherwise, the parsed
     * parameter is returned as an Object. This may be a String, Map, or List depending
     * on the parameter's structure.
     * 
     * @param moduleName    Name of module to get parameter for.
     * @param paramName     Name of parameter to get value of.
     * @return              Parameter value as an Object or null if known.
     */
    @SuppressWarnings("unchecked")
    public Object getModuleParam(String moduleName, String paramName) {
        Object moduleParams = getModuleParams(moduleName);
        if (moduleParams == null || !(moduleParams instanceof Map)) {
            return null;
        }
        return ((Map<String, ?>)moduleParams).get(paramName);
    }
    
    /**
     * Get the boolean value of the given parameter name belonging to the given module
     * name. If no such module/parameter name is known, false is returned. If the given
     * value is not a String, a RuntimeException is thrown. Boolean.parseBoolean() is
     * used to parse the value, which will return false if the String value is anything
     * other than "true" (case-insensitive).
     * 
     * @param moduleName    Name of module to get parameter for.
     * @param paramName     Name of parameter to get value of.
     * @return              True if the module and parameter exist with the value "true"
     *                      (case-insensitive), otherwise false.
     */
    @SuppressWarnings("unchecked")
    public boolean getModuleParamBoolean(String moduleName, String paramName) {
        Object moduleParams = getModuleParams(moduleName);
        if (moduleParams == null || !(moduleParams instanceof Map)) {
            return false;
        }
        Object value = ((Map<String, ?>)moduleParams).get(paramName);
        try {
            return Boolean.parseBoolean(value.toString());
        } catch (Exception e) {
            throw new RuntimeException("Configuration parameter '" + moduleName + "." +
                                        paramName + "' must be a Boolean: " + value);
        }
    }
    
    /**
     * Get the long value of the given parameter name belonging to the given module
     * name. If no such module/parameter name is known, zero is returned. If the given
     * value is not a long, a RuntimeException is thrown.
     * 
     * @param moduleName    Name of module to get parameter for.
     * @param paramName     Name of parameter to get value of.
     * @return              Parameter value as a long or null if known.
     */
    @SuppressWarnings("unchecked")
    public long getModuleParamLong(String moduleName, String paramName) {
        Object moduleParams = getModuleParams(moduleName);
        if (moduleParams == null || !(moduleParams instanceof Map)) {
            return 0;
        }
        Object value = ((Map<String, ?>)moduleParams).get(paramName);
        try {
            return Long.parseLong(value.toString());
        } catch (Exception e) {
            throw new RuntimeException("Configuration parameter '" + moduleName + "." +
                                       paramName + "' must be a number: " + value);
        }
    }

    /**
     * Get the value of the given parameter name belonging to the given module as integer.
     * If no such module/parameter exists, defaultValue is returned.
     * If the given value is not an integer, a RuntimeException is thrown.
     * 
     * @param moduleName    Name of module to get parameter for.
     * @param paramName     Name of parameter to get value of.
     * @return              Parameter value as a integer or defaultValue if tha parameter does not exis.
     */
    @SuppressWarnings("unchecked")
    public int getModuleParamInt(String moduleName, String paramName, int defaultValue) {
        Object moduleParams = getModuleParams(moduleName);
        if (moduleParams == null || !(moduleParams instanceof Map)) {
            return defaultValue;
        }
        Object value = ((Map<String, ?>)moduleParams).get(paramName);
        if(value == null) return defaultValue;
        try {
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            throw new RuntimeException("Configuration parameter '" + moduleName + "." +
                                       paramName + "' must be a number: " + value);
        }
    }
    
    /**
     * Get the String value of the given parameter name belonging to the given module
     * name. If no such module/parameter name is known, null is returned. If the parameter
     * exists, a string is created by calling toString().
     * 
     * @param moduleName    Name of module to get parameter for.
     * @param paramName     Name of parameter to get value of.
     * @return              Parameter value as a String or null if known.
     */
    @SuppressWarnings("unchecked")
    public String getModuleParamString(String moduleName, String paramName) {
        Object moduleParams = getModuleParams(moduleName);
        if (moduleParams == null || !(moduleParams instanceof Map)) {
            return null;
        }
        Object value = ((Map<String, ?>)moduleParams).get(paramName);
        if (value == null) {
            return null;
        }
        return value.toString();
    }
    
    /**
     * Get all configured parameters as a map. The map's keys are module names and its
     * parameters are Objects, which may be Strings, Maps, or Lists.
     * 
     * @return The map of the configuration properties.
     */
    public Map<String, Object> toMap() {
        return new HashMap<String, Object>(m_params);
    }

    /**
     * Return the command line arguments that were passed to {@link #load(String[])}.
     * 
     * @return  Command line arguments, if any.
     */
    public String[] getCommandLineArgs() {
        return m_commandLineArgs;
    }
    
    //----- Private methods
    
    // Inspect the classpath to find configuration file
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
                if (f.exists()) {
                    configUrl = new URL("file:///" + f.getCanonicalPath());
                }
            } catch( Exception ex) {
            }
        }

        if (configUrl == null) {
            ClassLoader loader = ServerParams.class.getClassLoader();
            configUrl = loader.getResource(spec);

            if (configUrl == null) {
                throw new ConfigurationException("Can't find file/resource: \"" + spec + "\".");
            }
        }

        return configUrl;
    }

    @SuppressWarnings("unchecked")
    private void parseConfigFile(URL url) throws ConfigurationException {
        try {
            logger.info("Loading configuration parameters from: {}", url);
            InputStream input = url.openStream();
            Yaml yaml = new Yaml();
            LinkedHashMap<String, ?> params = (LinkedHashMap<String, ?>)yaml.load(input);
            setConfigParams(params);
        } catch (Exception e) {
            logger.error("Failed to load configuration file", e);
            throw new ConfigurationException("Failed to load configuration file", e);
        }
    }
    
    // Set configuration parameters from the given YAML-loaded map.
    private void setConfigParams(Map<String, ?> map) throws ConfigurationException {
        for (String name : map.keySet()) {
            setConfigParam(name, map.get(name));
        }
    }
    
    // Set the given parameter name and value. The parameter name could be a dotted
    // module name (e.g., com.dell.doradus.service.db.DBService) or a legacy YAML file
    // option (e.g., dbhost).
    private void setConfigParam(String name, Object value) throws ConfigurationException {
        if (isModuleName(name)) {
            setModuleParams(name, value);
        } else {
            setLegacyParam(name, value);
        }
    }

    // Sets the given module's paramters to the given map. 
    private void setModuleParams(String moduleName, Object value) throws ConfigurationException {
        Utils.require(value instanceof Map, "Parameter for module %s should be a map: %s", moduleName, value.toString());
        m_params.put(moduleName, value);
    }

    // Sets a parameter using a legacy name.
    private void setLegacyParam(String legacyParamName, Object value) throws ConfigurationException {
        if (!m_bWarnedLegacyParam) {
            logger.warn("Parameter '{}': Legacy parameter format is being phased-out. " +
                        "Please use new module/parameter format.", legacyParamName);
            m_bWarnedLegacyParam = true;
        }
        String moduleName = g_legacyToModuleMap.get(legacyParamName);
        if (moduleName == null) {
            logger.warn("Skipping unknown legacy parameter: {}", legacyParamName);
        } else {
            setModuleParam(moduleName, legacyParamName, value);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void setModuleParam(String moduleName, String paramName, Object paramValue) {
        Map<String, Object> moduleMap = (Map<String, Object>)m_params.get(moduleName);
        if (moduleMap == null) {
            moduleMap = new HashMap<String, Object>();
            m_params.put(moduleName, moduleMap);
        }
        moduleMap.put(paramName, paramValue);
        
    }
    // Overrides the loaded/default settings by command line arguments.
    private void parseCommandLineArgs(String[] args) throws ConfigurationException {
        if (args == null || args.length == 0) {
            return;
        }
        logger.info("Parsing command line arguments");
        m_commandLineArgs = args;
        
        try {
            for (int inx = 0; inx < args.length; inx++) {
                String arg = args[inx];
                if (arg.equals("-?") || arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("-help")) {
                    System.out.println("See documentation and doradus.yaml for help.");
                    System.exit(0);
                }
                Utils.require(arg.charAt(0) == '-', "Unrecognized argument: %s", arg);
                Utils.require(inx + 1 < args.length, "A value is expected after: %s", arg);
                String name = arg.substring(1);
                String value = args[++inx];
                setCommandLineParam(name, value);
            }
        } catch (Exception e) {
            logger.error("Failed to parse command line arguments", e);
            throw new ConfigurationException("Failed to parse command line arguments", e);
        }
    }
    
    // Legacy format: "dbhost foo". New module format: "RESTService.dbhost foo".
    private void setCommandLineParam(String cmdArgName, String cmdArgValue) throws ConfigurationException {
        int dotInx = cmdArgName.indexOf('.');
        String moduleName = dotInx <= 0 ? null : cmdArgName.substring(0, dotInx);
        String paramName = dotInx <= 0 ? cmdArgName : cmdArgName.substring(dotInx + 1);
        String[] values = cmdArgValue.split(",");
        Object paramValue = values.length <= 1 ? cmdArgValue : Arrays.asList(values); 
        if (moduleName == null) {
            setLegacyParam(cmdArgName, paramValue);
        } else {
            setModuleParam(moduleName, paramName, paramValue);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void parseOverrideFile() throws ConfigurationException {
        String overrideFilename = getModuleParamString("DoradusServer", "param_override_filename");
        if (!Utils.isEmpty(overrideFilename)) {
            try {
                logger.info("Parsing parameter override file: {}", overrideFilename);
                InputStream overrideFile = new FileInputStream(overrideFilename);
                Yaml yaml = new Yaml();
                LinkedHashMap<String, ?> overrideColl = (LinkedHashMap<String, ?>)yaml.load(overrideFile);
                setConfigParams(overrideColl);
            } catch (Exception e) {
                logger.error("Failed to parse parameter override file", e);
                throw new ConfigurationException("Failed to parse command line arguments", e);
            }
        }
    }
    
    // An outer parameter name is a module name if it starts with an uppercase letter.
    private boolean isModuleName(String name) {
        return Character.isUpperCase(name.charAt(0));
    }
    
    // Print all configuration parameters to the log. 
    private void printConfig() {
        logger.debug("-----Configuration parameters:");
        for (String paramName : m_params.keySet()) {
            Object paramValue = m_params.get(paramName);
            printParam(paramName, paramValue, "");
        }
    }
    
    @SuppressWarnings("unchecked")
    private static void printParam(String paramName, Object paramValue, String indent) {
        if (paramValue instanceof List) {
            logger.debug(indent + paramName + " (list):");
            printListParams((List<?>)paramValue, indent+"   ");
        } else if (paramValue instanceof Map) {
            logger.debug(indent + paramName + " (map):");
            printMapParams((Map<String, ?>)paramValue, indent+"   ");
        } else {
            logger.debug(indent + paramName + ": " + paramValue);
        }
    }
    
    @SuppressWarnings("unchecked")
    private static void printListParams(List<?> values, String indent) {
        for (Object value : values) {
            if (value instanceof Map) {
                printMapParams((Map<String, ?>)value, indent);
            } else if (value instanceof List) {
                printListParams((List<?>)value, indent);
            } else {
                logger.debug(indent + value.toString());
            }
        }
    }
    
    private static void printMapParams(Map<String, ?> values, String indent) {
        for (String subParam : values.keySet()) {
            printParam(subParam, values.get(subParam), indent);
        }
    }
    
    // TODO: Delete when ServerConfig is phased out
    @SuppressWarnings("unchecked")
    private static void copyParamsToServerConfig() {
        ServerConfig.createInstance();
        for (String moduleName : INSTANCE.m_params.keySet()) {
            Map<String, Object> paramMap = (Map<String, Object>)INSTANCE.m_params.get(moduleName);
            for (String paramName : paramMap.keySet()) {
                ServerConfig.setParam(paramName, paramMap.get(paramName));
            }
        }
    }

}   // class ServerParams

