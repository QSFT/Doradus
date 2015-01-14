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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpDefs;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DuplicateException;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.db.UnauthorizedException;
import com.dell.doradus.service.tenant.TenantService;

/**
 * An HttpServlet implementation used by the {@link RESTService} to process REST requests.
 * Each request is mapped to a {@link RESTCommand} and processed via its callback. If the
 * the request is unknown or the callback creates 
 *
 */
public class RESTServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    //----- Inherited methods from HttpServlet
    
    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            long startNano = System.nanoTime();
            RESTService.instance().onNewrequest();

            // Extract the query component of the request, if any, but move _api and
            // _format parameters to the variable map if present.
            Map<String, String> variableMap = new HashMap<String, String>();
            String query = extractQueryParam(request, variableMap);
            RESTCommand cmd = RESTService.instance().matchCommand(request.getMethod(),
                                                                  request.getRequestURI(),
                                                                  query,
                                                                  variableMap);
            if (cmd == null) {
                throw new NotFoundException("Request does not match a known URI: " + request.getRequestURL());
            }
            Utils.require(cmd != null, "Request does not match a known URI: " + request.getRequestURL());
            m_logger.debug("Command: {}", cmd.toString());
            
            Tenant tenant = setTenant(cmd, request, variableMap);
            RESTRequest restRequest = new RESTRequest(tenant, request, variableMap);
            RESTCallback callback = cmd.getNewCallback(restRequest);
            RESTResponse restResponse = callback.invoke();
            
            if(restResponse.getCode().getCode() >= 300) {
                RESTService.instance().onRequestRejected(restResponse.getCode().toString());
            } else {
                RESTService.instance().onRequestSuccess(startNano);
            }
            sendResponse(response, restResponse);
        } catch (IllegalArgumentException e) {
            // 400 Bad Request
            RESTResponse restResponse = new RESTResponse(HttpCode.BAD_REQUEST, e.getMessage());
            RESTService.instance().onRequestRejected(restResponse.getCode().toString());
            sendResponse(response, restResponse);
        } catch (NotFoundException e) {
            // 404 Not Found
            RESTResponse restResponse = new RESTResponse(HttpCode.NOT_FOUND, e.getMessage());
            RESTService.instance().onRequestRejected(restResponse.getCode().toString());
            sendResponse(response, restResponse);
        } catch (DBNotAvailableException e) {
            // 503 Service Unavailable
            RESTResponse restResponse = new RESTResponse(HttpCode.SERVICE_UNAVAILABLE, e.getMessage());
            RESTService.instance().onRequestRejected(restResponse.getCode().toString());
            sendResponse(response, restResponse);
        } catch (UnauthorizedException e) {
            // 401 Unauthorized
            RESTResponse restResponse = new RESTResponse(HttpCode.UNAUTHORIZED, e.getMessage());
            RESTService.instance().onRequestRejected(restResponse.getCode().toString());
            sendResponse(response, restResponse);
        } catch (DuplicateException e) {
            // 409 Conflict
            RESTResponse restResponse = new RESTResponse(HttpCode.CONFLICT, e.getMessage());
            RESTService.instance().onRequestRejected(restResponse.getCode().toString());
            sendResponse(response, restResponse);
        } catch (Throwable e) {
            // 500 Internal Error: include a stack trace and report in log.
            m_logger.error("Unexpected exception handling request", e);
            String stackTrace = Utils.getStackTrace(e);
            RESTResponse restResponse = new RESTResponse(HttpCode.INTERNAL_ERROR, stackTrace);
            RESTService.instance().onRequestFailed(e);
            sendResponse(response, restResponse);
        }
    }    // goGet
    
    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }    // doPost
    
    /**
     * @see HttpServlet#doPut(HttpServletRequest request, HttpServletResponse response)
     */
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }    // doPut
    
    /**
     * @see HttpServlet#doDelete(HttpServletRequest request, HttpServletResponse response)
     */
    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }    // doDelete
    
    //----- Private methods

    // Decide the Tenant context for this command. System commands execute with no tenant.
    // In single-tenant mode, the Tenant is always the default keyspace.
    private Tenant setTenant(RESTCommand cmd, HttpServletRequest request, Map<String, String> variableMap) {
        Tenant tenant = null;
        if (ServerConfig.getInstance().multitenant_mode) {
            String authorizationHeader = request.getHeader("Authorization");
            if (cmd.isSystemCommand()) {
                // TODO: Should tenant be null for system commands?
                tenant = TenantService.instance().validateSystemUser(authorizationHeader);
            } else {
                String tenantName = Utils.urlDecode(variableMap.get("tenant"));
                if (tenantName == null) {
                    throw new RuntimeException("Non-system RESTCommand missing 'tenant' variable: " + cmd);
                }
                tenant = TenantService.instance().validateTenant(tenantName, authorizationHeader);
            }
        } else {
            tenant = TenantService.instance().getDefaultTenant();
        }
        return tenant;
    }   // setTenant
    
    // Send the given response, which includes a response code and optionally a body
    // and/or additional response headers. If the body is non-empty, we automatically add
    // the Content-Length and a Content-Type of Text/plain.
    private void sendResponse(HttpServletResponse servletResponse, RESTResponse restResponse) throws IOException {
        servletResponse.setStatus(restResponse.getCode().getCode());
        
        Map<String, String> responseHeaders = restResponse.getHeaders();
        if (responseHeaders != null) {
            for (Map.Entry<String, String> mapEntry : responseHeaders.entrySet()) {
                if (mapEntry.getKey().equalsIgnoreCase(HttpDefs.CONTENT_TYPE)) {
                    servletResponse.setContentType(mapEntry.getValue());
                } else {
                    servletResponse.setHeader(mapEntry.getKey(), mapEntry.getValue());
                }
            }
        }
        
        byte[] bodyBytes = restResponse.getBodyBytes();
        int bodyLen = bodyBytes == null ? 0 : bodyBytes.length;
        servletResponse.setContentLength(bodyLen);
        
        if (bodyLen > 0 && servletResponse.getContentType() == null) {
            servletResponse.setContentType("text/plain");
        }
        
        if (bodyLen > 0) {
            servletResponse.getOutputStream().write(restResponse.getBodyBytes());
        }
    }   // sendResponse
    
    // Extract and return the query component of the given request, but move "api=x" and
    // "format=y", if present, to rest parameters called _api and _format.
    private String extractQueryParam(HttpServletRequest request, Map<String, String> restParams) {
        String query = request.getQueryString();
        if (Utils.isEmpty(query)) {
            return "";
        }
        StringBuilder buffer = new StringBuilder(query);
        
        // Split query component into decoded, &-separate components.
        String[] parts = Utils.splitURIQuery(buffer.toString());
        List<Pair<String, String>> unusedList = new ArrayList<Pair<String,String>>();
        boolean bRewrite = false;
        for (String part : parts) {
            Pair<String, String> param = extractParam(part);
            if (param.left.equalsIgnoreCase("api")) {
                restParams.put("_api", param.right);
                bRewrite = true;
            } else if (param.left.equalsIgnoreCase("format")) {
                bRewrite = true;
                if (param.right.equalsIgnoreCase("xml")) {
                    restParams.put("_format", "text/xml");
                } else if (param.right.equalsIgnoreCase("json")) {
                    restParams.put("_format", "application/json");
                }
            } else {
                unusedList.add(param);
            }
        }
        
        // If we extracted any fixed params, rewrite the query parameter.
        if (bRewrite) {
            buffer.setLength(0);
            for (Pair<String, String> pair : unusedList) {
                if (buffer.length() > 0) {
                    buffer.append("&");
                }
                buffer.append(Utils.urlEncode(pair.left));
                if (pair.right != null) {
                    buffer.append("=");
                    buffer.append(Utils.urlEncode(pair.right));
                }
            }
        }
        return buffer.toString();
    }   // extractQueryParam

    // Split the given k=v (or just k) param into a Pair object.
    private Pair<String, String> extractParam(String part) {
        int eqInx = part.indexOf('=');
        String paramName;
        String paramValue;
        if (eqInx < 0) {
            paramName = part;
            paramValue = null;
        } else {
            paramName = part.substring(0, eqInx);
            paramValue = part.substring(eqInx + 1);
        }
        return Pair.create(paramName, paramValue);
    }   // extractParam
    
}   // class RESTServlet
