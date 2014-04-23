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
 * Static HTTP header definitions. Only headers used by the Doradus REST API are define.
 */
public final class HttpDefs {
    // Commonly used headers up-cased.
    public static final String ACCEPT           = "ACCEPT";
    public static final String ACCEPT_ENCODING  = "ACCEPT-ENCODING";
    public static final String API_VERSION      = "X-API-VERSION";
    public static final String CONTENT_ENCODING = "CONTENT-ENCODING";
    public static final String CONTENT_LENGTH   = "CONTENT-LENGTH";
    public static final String CONTENT_TYPE     = "CONTENT-TYPE";
    public static final String EXPECT           = "EXPECT";
    public static final String HOST             = "HOST";
    
    // No construction allowed.
    private HttpDefs() {
        throw new AssertionError();
    }   // constructor

}   // HttpDefs
