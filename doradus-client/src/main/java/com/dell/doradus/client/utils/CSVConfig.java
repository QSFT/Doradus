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

package com.dell.doradus.client.utils;

import java.lang.reflect.Field;

import com.dell.doradus.client.SSLTransportParameters;

/**
 * Holds configuration parameters for the CSVLoader and CSVDumper apps. The singleton
 * instance is obtained via {@link #instance()}. All options have defaults and can be
 * overridden via direct access or via {@link #set(String, String)}. If used, TLS
 * parameters can be retrieved via {@link #getTLSParams()}.
 */
public class CSVConfig {
    // Defaults:
    public static final String  DEFAULT_APPNAME         = "BIM";
    public static final int     DEFAULT_BATCH_SIZE      = 1000;
    public static final boolean DEFAULT_COMPRESS        = true;
    public static final String  DEFAULT_HOST            = "localhost";
    public static final String  DEFAULT_ID_FIELD        = "Key";
    public static final boolean DEFAULT_INCREMENT_TS    = false;
    public static final boolean DEFAULT_MERGE_ALL       = false;
    public static final boolean DEFAULT_OPTIMIZE        = false;
    public static final int     DEFAULT_PORT            = 1123;
    public static final String  DEFAULT_ROOT            = ".";
    public static final String  DEFAULT_SCHEMA          = "BIM.xml";
    public static final boolean DEFAULT_SKIP_UNDEF      = false;
    public static final String  DEFAULT_SHARD           = "s1";
    public static final int     DEFAULT_WORKERS         = 3;
    
    // TLS defaults:
    public static final boolean DEFAULT_TLS             = false;
    public static final String  DEFAULT_KEYSTORE        = "config/keystore";
    public static final String  DEFAULT_KEYSTORE_PW     = "changeit";
    public static final String  DEFAULT_TRUSTSTORE      = "config/truststore";
    public static final String  DEFAULT_TRUSTSTORE_PW   = "password";
    
    // Configuration properties
    public String   app                 = DEFAULT_APPNAME;
    public int      batchsize           = DEFAULT_BATCH_SIZE;
    public boolean  compress            = DEFAULT_COMPRESS;
    public String   host                = DEFAULT_HOST;
    public String   id                  = DEFAULT_ID_FIELD;
    public boolean  increment_ts        = DEFAULT_INCREMENT_TS;
    public String   keystore            = DEFAULT_KEYSTORE;
    public String   keystorepassword    = DEFAULT_KEYSTORE_PW;
    public boolean  merge_all           = DEFAULT_MERGE_ALL;
    public boolean  optimize            = DEFAULT_OPTIMIZE;
    public int      port                = DEFAULT_PORT;
    public String   root                = DEFAULT_ROOT;
    public String   schema              = DEFAULT_SCHEMA;
    public boolean  skip_undef          = DEFAULT_SKIP_UNDEF;
    public String   shard               = DEFAULT_SHARD;
    public boolean  tls                 = DEFAULT_TLS;
    public String   truststore          = DEFAULT_TRUSTSTORE;
    public String   truststorepassword  = DEFAULT_TRUSTSTORE_PW;
    public int      workers             = DEFAULT_WORKERS;

    // TLS parameters if used:
    private SSLTransportParameters sslParams;

    // Singleton instance
    private static final CSVConfig INSTANCE = new CSVConfig();
    private CSVConfig() {}
    public static CSVConfig instance() {return INSTANCE;}
    
    /**
     * Set the given configuration option name to the given value. The value's format must
     * be compatible with the option's type (String, int, or boolean). If the given option
     * name is unknown, an exception is thrown.
     * 
     * @param name  Option name (e.g., "app", "batchsize")
     * @param value Option value (e.g., "Email", "10000", "true").
     */
    public void set(String name, String value) {
        try {
            Field field = CSVConfig.class.getDeclaredField(name);
            if (field.getType().toString().compareToIgnoreCase("int") == 0) {
                field.set(this, Integer.parseInt(value));
            } else if (field.getType().toString().compareToIgnoreCase("boolean") == 0) {
                if (value.equalsIgnoreCase("t")) value = "true";
                field.set(this, Boolean.parseBoolean(value));
            } else {
                field.set(this, value);
            }
        } catch (NoSuchFieldException | SecurityException e) {
           throw new IllegalArgumentException("Unknown parameter: " + name);
        } catch (Exception e) {
           throw new IllegalArgumentException("Invalid value for '" + name + "': " + value);
        }
    }   // set
    
    /**
     * If the 'tls' option is true, return the TLS parameters packaged in an 
     * {@link SSLTransportParameters} object.
     */
    public SSLTransportParameters getTLSParams() {
        if (tls && sslParams == null) { 
            sslParams = new SSLTransportParameters();
            sslParams.setKeyStore(keystore, keystorepassword);
            sslParams.setTrustStore(truststore, truststorepassword);        
        }
        return sslParams;
    }   // getTLSParams

}   // class CSVConfig
