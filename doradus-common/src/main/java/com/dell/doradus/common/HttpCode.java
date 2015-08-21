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
 * Represents HTTP response codes and the associated tag string (e.g., "200 OK"). Not
 * all return codes from RFC 2616 are defined: only those used by Doradus. 
 */
public enum HttpCode {
    // 1xx Codes
    CONTINUE(100, "Continue"),
    
    // 2xx Codes
    OK(200, "OK"),
    CREATED(201,"Created"),
    NO_CONTENT(204, "No Content"),
    
    // 4xx Codes
    BAD_REQUEST(400, "Bad Request"),
    UNAUTHORIZED(401, "Unauthorized"),
    NOT_FOUND(404, "Not Found"),
    CONFLICT(409, "Conflict"),
    
    // 5xx Codes
    INTERNAL_ERROR(500, "Internal Server Error"),
    NOT_IMPLEMENTED(501, "Not Implemented"),
    SERVICE_UNAVAILABLE(503, "Service Unavailable");
    
    // Members:
    final int    code;
    final String tag;
    
    /**
     * Create an HttpCode with the given code and tag. The codes and tags are
     * defined by RFC 2616.
     * 
     * @param code  Valid HTTP code (e.g., 200).
     * @param tag   Code information tag (e.g., Continue).
     */
    HttpCode(int code, String tag) {
        this.code = code;
        this.tag = tag;
    }   // constructor
    
    /**
     * Get the numeric value of this HttpCode.
     * 
     * @return  The numeric value of this HttpCode.
     */
    public int getCode() { return code; }

    /**
     * Return true if this HttpCode represents an error.
     * 
     * @return  True if this HttpCode's is &gt;= 400.
     */
    public boolean isError() {
        return code >= 400;
    }   // isError
    
    /**
     * Returns returns the string "code tag".
     * 
     * @return The string "code tag".
     */
    @Override
    public String toString() {
        return Integer.toString(code) + " " + tag;
    }   // toString

    // Map of all codes we use by integer value.
    private static Map<Integer, HttpCode> g_codeMap = new HashMap<Integer, HttpCode>();
    
    // Initialize the map from all codes defined.
    static {
        for (HttpCode httpCode : values()) {
            g_codeMap.put(httpCode.code, httpCode);
        }
    }
    
    /**
     * Return the {@link HttpCode} with the given code value. If the given code is not
     * recognized (not used by Doradus), null is returned.
     * 
     * @param   code    Value of an HTTP return code.
     * @return          Corresponding HttpCode enumeration or null if unknown.
     */
    public static HttpCode findByCode(int code) {
        return g_codeMap.get(code);
    }   // findByCode
    
}   // enum HttpCode
