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

package com.dell.doradus.service.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;

import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.HttpDefs;
import com.dell.doradus.common.Utils;

/**
 * Wrapper for an HTTP request (as an {@link HttpServletRequest} and the variables, if
 * any, extracted from the request URI. Provides convenience methods to access request
 * headers and parameters, including getting the input message body as a String, byte[],
 * or InputStream.
 */
public class RESTRequest {
    // Request members that this object wraps:
    private final HttpServletRequest    m_request;
    private final Map<String, String>   m_variableMap;

    // Extracted members for easy access:
    private ContentType   m_contentTypeIn;      // from Content-Type
    private ContentType   m_contentTypeOut;     // from Accept-Type
    private byte[]        m_requestEntity;      // raw input message (may be compressed)
    private boolean       m_bEntityCompressed;  // true if content entity is compressed

    /**
     * Create an object that wraps the given request parameters.
     * 
     * @param request       Request as received by Servlet interface.
     * @param variableMap   Variables extracted from the REST URI (should be decoded).
     * @throws IOException 
     */
    public RESTRequest(HttpServletRequest request, Map<String, String> variableMap) {
        m_request = request;
        m_variableMap = variableMap;
        setRequestMembers();
    }   // constructor

    /**
     * Get the variables extracted for this REST request. For example, if the
     * {@link RESTCommand} for this request specified the URI:
     * <pre>
     *      /{application}/{table}/_query?{query}
     * </pre>
     * and the actual request used in this request is:
     * <pre>
     *      /foo/bar/_query?q=cat+dog
     * </pre>
     * The variables returned by this method consists of the following key/value pairs:
     * <pre>
     *      application=foo
     *      table=bar
     *      query=q=cat+dog
     * </pre>
     * Note that variable values are not URI-decoded since they may need processing before
     * being decoded.
     *  
     * @return  A map of variables extracted for this REST request, if any, URI-encoded.
     *          The map will be empty if there are no variables defined by this request's
     *          {@link RESTCommand}. 
     */
    public Map<String, String> getVariables() {
        return m_variableMap;
    }   // getVariables

    /**
     * Get the undecoded value of the variable with the given name. Null is returned if
     * there is no variable with the given name.
     * 
     * @param variableName  Variable name.
     * @return              The value of requested variable URI-decoded, or null if there
     *                      is no such variable.
     * @see #getVariables()
     * @see #getVariableDecoded(String)
     */
    public String getVariable(String variableName) {
        return m_variableMap.get(variableName);
    }   // getVariable
    
    /**
     * Get the URI-decoded value of the variable with the given name. Null is returned if
     * there is no variable with the given name. This method calls
     * {@link #getVariable(String)} and passes the result through
     * {@link Utils#urlDecode(String)}.
     * 
     * @param variableName  Variable name.
     * @return              The value of requested variable URI-decoded, or null if there
     *                      is no such variable.
     * @see #getVariables()
     * @see #getVariable(String)
     */
    public String getVariableDecoded(String variableName) {
        String value = m_variableMap.get(variableName);
        if (value == null) {
            return null;
        }
        return Utils.urlDecode(value);
    }   // getVariable
    
    /**
     * Get the value of the content-length header for this REST request. It could be 0 or
     * even -1 if the request has no input entity.
     *  
     * @return  The value of the content-length header for this REST request.
     */
    public int getContentLength() {
        return m_request.getContentLength();
    }   // getContentLength
    
    /**
     * Get the content-type header of this REST request as a {@link ContentType}. If no
     * content-type was specified, the default content-type is returned.
     * 
     * @return  The content-type of this REST request as a {@link ContentType}.
     */
    public ContentType getInputContentType() {
        return m_contentTypeIn;
    }   // getInputContentType
    
    /**
     * Get the accept header of this REST request as a {@link ContentType}. If no accept
     * header was specified, the same value as {@link #getInputContentType()} is returned.
     * 
     * @return  The accept header of this REST request as a {@link ContentType}.
     */
    public ContentType getOutputContentType() {
        return m_contentTypeOut;
    }   // getOutputContentType
    
    /**
     * Get the input entity (body) of this REST request as a string. If no input entity
     * was provided, an empty string is returned. If an input entity exists and is
     * compressed, it is decompressed first. The binary input entity is converted to a
     * string using UTF-8.
     * 
     * @return  The input entity (body) of this REST request as a string. It is empty if
     *          there is no input string.
     */
    public String getInputBody() {
        if (m_requestEntity.length == 0 || !m_bEntityCompressed) {
            return Utils.toString(m_requestEntity);
        } else {
            try {
                return Utils.toString(Utils.decompressGZIP(m_requestEntity));
            } catch (IOException e) {
                throw new IllegalArgumentException("Error decompressing input (malformed?)", e);
            }
        }
    }   // getInputBody
    
    /**
     * Get the input entity (body) of this REST request as an InputStream. If no input
     * entity was provided, null is returned. If an input entity is present and is
     * compressed, the stream returned is the decompressed byte stream.

     * @return  The input entity (body) of this REST request as an InputStream, returning
     *          decompressed bytes if necessary. Null is returned if there is no input
     *          stream.
     */
    public InputStream getInputStream() {
        if (m_requestEntity.length == 0) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(m_requestEntity);
        InputStream inStream = bis;
        if (m_bEntityCompressed) {
            try {
                inStream = new GZIPInputStream(bis);
            } catch (IOException e) {
                throw new IllegalArgumentException("Error decompressing input (malformed?)", e);
            }
        }
        return inStream;
    }   // getInputStream
    
    ///// Private methods

    // Extract convenience members, which could detect problems with input parameters.
    private void setRequestMembers() {
        m_contentTypeIn = getContentType();
        m_contentTypeOut = getAcceptType();
        m_requestEntity = readRequestBody();
        m_bEntityCompressed = isMessageCompressed();
    }   // setRequestMembers

    // Get the request's content-type, using XML as the default.
    private ContentType getContentType() {
        String contentTypeValue = m_request.getContentType();
        if (contentTypeValue == null) {
            return ContentType.TEXT_XML;
        }
        return new ContentType(contentTypeValue);
    }   // getContentType
    
    // Get the request's accept type, defaulting to content-type if none is specified.
    private ContentType getAcceptType() {
        // If the _format header is present, it overrides the ACCEPT header.
        String format = m_variableMap.get("_format");
        if (format != null) {
            return new ContentType(format);
        }
        String acceptParts = m_request.getHeader(HttpDefs.ACCEPT);
        if (!Utils.isEmpty(acceptParts)) {
	        for (String acceptPart : acceptParts.split(",")) {
	            ContentType acceptType = new ContentType(acceptPart);
	            if (acceptType.isJSON() || acceptType.isXML()) {
	                return acceptType;
	            }
	        }
        }
        return getContentType();
    }   // getAcceptType
    
    // If Content-Encoding is included, verify that we support it and return true.
    private boolean isMessageCompressed() {
        String contentEncoding = m_request.getHeader(HttpDefs.CONTENT_ENCODING);
        if (contentEncoding != null) {
            if (!contentEncoding.equalsIgnoreCase("gzip")) {
                throw new IllegalArgumentException("Unsupported Content-Encoding: " + contentEncoding);
            }
            return true;
        }
        return false;
    }   // isMessageCompressed

    // Extract the input entity (body) of this request. If there is no input entity, a
    // zero-length byte[] is returned. The message is *not* decompressed.
    private byte[] readRequestBody() {
        int bytesLeft = m_request.getContentLength();
        if (bytesLeft <= 0) {
            return new byte[0];
        }

        try {
            byte[] byteBuffer = new byte[bytesLeft];
            try (InputStream inputStream = m_request.getInputStream()) {
                int bytesRead = -1;
                int offset = 0;
                while ((bytesRead = inputStream.read(byteBuffer, offset, bytesLeft)) > 0) {
                    offset += bytesRead;
                    bytesLeft -= bytesRead;
                }
            }
            return byteBuffer;
        } catch (IOException e) {
            throw new RuntimeException("Error reading from input request", e);
        }
    }   // readRequestBody
    
}   // class RESTRequest
