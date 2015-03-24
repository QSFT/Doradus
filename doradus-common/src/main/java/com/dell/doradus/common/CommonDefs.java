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

package com.dell.doradus.common;

/**
 * Contains static definitions common client and server modules.
 */
final public class CommonDefs {
    // Static definitions only
    private CommonDefs() {
        assert false;
    }

    /**
     * First letter with which 'alias' names must start.
     */
    public static final char ALIAS_FIRST_CHAR = '$';

    /**
     * The name of the system field used to store object IDs.
     */
    public static final String ID_FIELD           = "_ID";

    /**
     * The character we use to separate values of an MV scalar.
     */
    public static final String MV_SCALAR_SEP_CHAR = "\uFFFE";

    /**
     * The application-level option that enables implicit table creation.
     */
    public static final String AUTO_TABLES = "AutoTables";

    /**
     * The application- and table-level option that specifies the frequency at which the
     * aging task should be performed.
     */
    public static final String OPT_AGING_CHECK_FREQ = "aging-check-frequency";

    /**
     * The table-level aging-field option.
     */
    public static final String OPT_AGING_FIELD = "aging-field";
    
    /**
     * The table-level retention-age option.
     */
    public static final String OPT_RETENTION_AGE = "retention-age";
    
    /**
     * The table-level sharding-field option.
     */
    public static final String OPT_SHARDING_FIELD = "sharding-field";
    
    /**
     * The table-level sharding-granularity option.
     */
    public static final String OPT_SHARDING_GRANULARITY = "sharding-granularity";
    
    /**
     * The table-level sharding-start option.
     */
    public static final String OPT_SHARDING_START = "sharding-start";
    
    /**
     * The application-level option that indicates the owning storage service.
     */
    public static final String OPT_STORAGE_SERVICE = "StorageService";
    
    /**
     * The application-level option that indicates the application's tenant. (See by the
     * server and should not be explicitly declared in schemas.)
     */
    public static final String OPT_TENANT = "Tenant";
    
    /**
     * System fields
     */
    public static enum SystemFields {
        _ID,
        _all,
        _local
    }
    
}   // CommonDefs
