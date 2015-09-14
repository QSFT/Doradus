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
    SWITCHING_PROTOCOLS(101, "Switching Protocols"),
    
    // 2xx Codes
    OK(200, "OK"),
    CREATED(201,"Created"),
    ACCEPTED(202, "Accepted"),
    NON_AUTHORITATIVE_INFORMATION(203, "Non-Authoritative Information"), 
    NO_CONTENT(204, "No Content"),
    RESET_CONTENT(205, "Reset Content"),
    PARTIAL_CONTENT(206, "Partial Content"),
    
    // 4xx Codes
    BAD_REQUEST(400, "Bad Request"),
    UNAUTHORIZED(401, "Unauthorized"),
    PAYMENT_REQUIRED(402, "Payment Required"),
    FORBIDDEN(403, "Forbidden"),
    NOT_FOUND(404, "Not Found"),
    METHOD_NOT_ALLOWED(405, "Method Not Allowed"),
    NOT_ACCEPTABLE(406, "Not Acceptable"),
    PROXY_AUTHENTICATION_REQUIRED(407, "Proxy Authentication Required"),
    REQUEST_TIMEOUT(408, "Request Timeout"),
    CONFLICT(409, "Conflict"),
    GONE(410, "Gone"),
    LENGTH_REQUIRED(411, "Length Required"),
    PRECONDITION_FAILED(412, "Precondition Failed"),
    REQUEST_ENTITY_TOO_LARGE(413, "Request Entity Too Large"),
    REQUEST_URI_TOO_LARGE(414, "Request-URI Too Long"),
    UNSUPPORTED_MEDIA_TYPE(415, "Unsupported Media Type"),
    REQUESTED_RANGE_NOT_SATISFIABLE(416, "Requested Range Not Satisfiable"),
    EXPECTATION_FAILED(417, "Expectation Failed"),
    
    // 5xx Codes
    INTERNAL_ERROR(500, "Internal Server Error"),
    NOT_IMPLEMENTED(501, "Not Implemented"),
    SERVICE_UNAVAILABLE(503, "Service Unavailable"),
    GATEWAY_TIMEOUT(504, "Gateway Timeout"),
    HTTP_VERSION_NOT_SUPPORTED(505, "HTTP Version Not Supported");
    
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
