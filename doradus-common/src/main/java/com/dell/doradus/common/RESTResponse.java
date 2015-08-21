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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an HTTP response, including the HTTP status code, message body (entity), and
 * additional response headers.
 */
public class RESTResponse {
    // Members:
    private final HttpCode m_statusCode;
    private final byte[] m_bodyBytes;
    private final Map<String, String> m_headers;

    /**
     * Create a response with a code only. The response will contain no body or additional
     * headers.
     * 
     * @param statusCode  HTTP status code to use in reponse.
     */
    public RESTResponse(HttpCode statusCode) {
        m_statusCode = statusCode;
        m_bodyBytes = null;
        m_headers = null;
    }   // constructor

    /**
     * Create a response with the given HTTP status code and body as a String. The response
     * will contain no extra headers. The body's content-type will be text/plain.
     * 
     * @param statusCode  HTTP status code to use in response.
     * @param body        Body to be included in response (copied).
     */
    public RESTResponse(HttpCode statusCode, String body) {
        m_statusCode = statusCode;
        m_bodyBytes = body == null ? null : Utils.toBytes(body);
        m_headers = null;
    }   // constructor

    /**
     * Create a response with the given HTTP status code, String body, and the given
     * content-type header.
     * 
     * @param statusCode    HTTP status code to use in response.
     * @param body          Body to be included in response (copied).
     * @param contentType   {@link ContentType} value to use for content-type header.
     */
    public RESTResponse(HttpCode statusCode, String body, ContentType contentType) {
        m_statusCode = statusCode;
        m_bodyBytes = body == null ? null : Utils.toBytes(body);
        m_headers = new HashMap<>();
        m_headers.put(HttpDefs.CONTENT_TYPE, contentType.toString());
    }   // constructor
    
    /**
     * Create a response with the given HTTP status, binary body, and additional
     * headers.
     * 
     * @param statusCode    HTTP status code to use in response.
     * @param body          Body to be included in the response (copied).
     * @param headers       Additional headers to be included in response.
     */
    public RESTResponse(HttpCode statusCode, byte[] body, Map<String, String> headers) {
        m_statusCode = statusCode;
        m_bodyBytes = body == null ? null : Arrays.copyOf(body, body.length);
        
        m_headers = new HashMap<String, String>(headers);
        if (headers != null) {
            // Copy headers as up-cased.
            for (Map.Entry<String, String> mapEntry : headers.entrySet()) {
                m_headers.put(mapEntry.getKey().toUpperCase(), mapEntry.getValue());
            }
        }
    }   // constructor
  
    ///// getters
    
    /**
     * Get this response's HTTP status code (e.g., 200 = OK).
     * 
     * @return  This response's HTTP status code.
     */
    public HttpCode getCode() {
        return m_statusCode;
    }   // getCode
    
    /**
     * Get this response's message body (entity) as a String. The binary message body is
     * converted to a String using UTF-8. If there is no message body, an empty string is
     * returned.
     *  
     * @return  This response's entity as a String or an empty string if there isn't one.
     */
    public String getBody() {
        return m_bodyBytes == null ? "" : Utils.toString(m_bodyBytes);
    }   // getBody
    
    /**
     * Get the content-type header from this response as a {@link ContentType} object.
     *  
     * @return  The {@link ContentType} header from this response or null if there isn't one.
     */
    public ContentType getContentType() {
        String contentType = m_headers.get(HttpDefs.CONTENT_TYPE);
        if (contentType == null) {
            return null;
        }
        return new ContentType(contentType);
    }   // getContentType
    
    /**
     * Get this response's binary response body, if any.
     * 
     * @return  This response's binary body or null if there is none.
     */
    public byte[] getBodyBytes() {
        return m_bodyBytes;
    }   // getBodyBytes
    
    /**
     * Get this response's headers as a field/value map. Note that the headers are not
     * copied so the caller shouldn't modify these.
     * 
     * @return  This response's headers as a field/value map. The map may be null or
     *          empty if there are no additional headers in this response.
     */
    public Map<String, String> getHeaders() {
        return m_headers;
    }   // getHeaders
    
    /**
     * Return true if this response's code is considered an error case.
     * 
     * @return  True if {@link #getCode()} is 400 or higher.
     */
    public boolean isFailed() {
        return m_statusCode.isError();
    }
    
    /**
     * Return a summary of this response object. If the response's HTTP code is
     * {@link HttpCode#OK}, then just just "{code}" is returned. If any other code is
     * present, the result is "{code}: {body}". In both cases, {code} is whatever is
     * returned by {@link HttpCode#toString()}.
     * 
     * @return The string "{code}" or "{code}: {body}".
     */
    @Override
    public String toString() {
        if (m_statusCode != HttpCode.OK && m_bodyBytes.length > 0) {
            return m_statusCode.toString() + ": " + Utils.toString(m_bodyBytes);
        }
        return m_statusCode.toString();
    }   // toString

}   // class RESTResponse
