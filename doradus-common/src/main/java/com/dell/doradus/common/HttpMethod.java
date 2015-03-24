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

import java.util.HashMap;
import java.util.Map;

/**
 * Enum class for HTTP methods (GET, PUT, etc.) Only methods used by the Doradus REST
 * protocol are defined.
 */
public enum HttpMethod {
    // We only list the ones we use currently.
    GET, POST, PUT, DELETE;
    
    // Map of all known methods by up-cased name (even though enum names are usually
    // up-cased by convention).
    private static final Map<String, HttpMethod> METHOD_NAMES =
        new HashMap<String, HttpMethod>();
    
    // The static initializer is called after all enum objects are constructed. Hence,
    // we can use the static values() method to iterate them an build the map. 
    static {
        for (HttpMethod method : values()) {
            METHOD_NAMES.put(method.toString().toUpperCase(), method);
        }
    }

    /**
     * Return the HttpMethod object associated with the given case-insensitive enum
     * name or null if the given name is unknown.
     * 
     * @param   methodName Case-insensitive name of an HTTP method (e.g. "Get").
     * @return             Corresponding HttpMethod enum, if recognized, else null.
     */
    public static HttpMethod methodFromString(String methodName) {
        return METHOD_NAMES.get(methodName.toUpperCase());
    }   // methodFromString
    
}   // enum HttpMethod
