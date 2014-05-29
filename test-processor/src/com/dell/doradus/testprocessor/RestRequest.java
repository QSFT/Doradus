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

package com.dell.doradus.testprocessor;

import com.dell.doradus.testprocessor.common.*;

public class RestRequest
{
    private String  m_requestFormat  = null;
    private String  m_responseFormat = null;
    private String  m_rest = null;
    private String  m_body = null;

    public void setRequestFormat(String value) {
    	if (value != null) value = value.toLowerCase();
    	m_requestFormat = value;
    }
    public String getRequestFormat() {
    	return m_requestFormat;
    }

    public void setResponseFormat(String value) {
    	if (value != null) value = value.toLowerCase();
    	m_responseFormat = value;
    }
    public String getResponseFormat() {
    	return m_responseFormat;
    }

    public void setRest(String value) {
    	m_rest = value;
    }
    public String getRest() {
    	return m_rest;
    }

    public void setBody(String value) {
    	m_body = value;
    }
    public String getBody() {
    	return m_body;
    }
    
    public String getContentType()
    {
        if (m_requestFormat == null || m_requestFormat.isEmpty())
            return "";
        if (m_requestFormat.equals("xml"))
            return "text/xml";
        if (m_requestFormat.equals("json"))
            return "application/json";
        return m_requestFormat;
    }
    
    public String getAcceptType()
    {
        if (m_responseFormat == null || m_responseFormat.isEmpty())
            return "";
        if (m_responseFormat.equals("xml"))
            return "text/xml";
        if (m_responseFormat.equals("json"))
            return "application/json";
        return m_responseFormat;
    }

    public String toString(String prefix)
    throws Exception
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();

        result.append(prefix + Utils.urlDecode(m_rest));
        if (m_body == null || m_body.isEmpty())
            return result.toString();

        result.append(Utils.EOL);
        if (m_requestFormat.equals("xml")) {
            result.append(XmlUtils.formatXml(m_body, prefix));
        }
        else if (m_requestFormat.equals("json")) {
            result.append(JsonUtils.formatJson(m_body, prefix));
        }
        else {
            result.append(StringUtils.formatText(m_body, prefix));
        }
        return result.toString();
    }
}
