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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.dell.doradus.testprocessor.common.*;

public class RestResponse
{
    private Map<String, String> m_header;
    private int                 m_code;
    private String              m_tag;
    private int                 m_contentLength;
    private String              m_contentType;
    private String              m_body;

    public RestResponse() {
        m_header = new HashMap<String, String>();
        m_code          = -1;
        m_tag           = "";
        m_contentLength = 0;
        m_contentType   = "";
        m_body          = "";
    }

    public int getCode() {
        return m_code;
    }
    public String getTag() {
        return m_tag;
    }
    public int getContentLength() {
        return m_contentLength;
    }
    public String getContentType() {
        return m_contentType;
    }
    public void setBody(String body) {
        m_body = body;
    }
    public String getBody() {
        return m_body;
    }

    public void parseHeader(List<String> headerLines)
    throws Exception
    {
        for (int i = 0; i < headerLines.size(); i++)
        {
            String line = headerLines.get(i);
            
            try {
                if (i == 0) {
                    parseStatusLine(line);
                    continue;
                }

                int colonPos = line.indexOf(':');

                String name = // use the whole line if there's no colon
                        colonPos < 1 ? line : line.substring(0, colonPos);
                name = name.toLowerCase().trim();

                String value = // use an empty string if there's no colon
                        colonPos < 1 ? "" : line.substring(colonPos + 1);
                value = value.trim();

                if (name.equals("content-length")) {
                    m_contentLength = Integer.parseInt(value);
                    continue;
                }
                if (name.equals("content-type")) {
                    if (value.toLowerCase().indexOf("xml") > -1)
                        m_contentType = "xml";
                    else if (value.toLowerCase().indexOf("json") > -1)
                        m_contentType = "json";
                    else if (value.toLowerCase().indexOf("text") > -1)
                        m_contentType = "text";
                    else
                        m_contentType = value;
                    continue;
                }

                m_header.put(name, value);
            }
            catch(Exception ex) {
                String msg = "Invalid header line: \"" + line + "\"";
                throw new Exception(msg, ex);
            }
        }
    }

    public String toString(String prefix)
    throws Exception
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();

        result.append(prefix + m_code + " " + m_tag);

        if (m_body != null && !m_body.isEmpty())
        {
            result.append(Utils.EOL);

            if ("xml".equals(m_contentType)) {
                result.append(XmlUtils.formatXml(m_body, prefix));
            }
            else if ("json".equals(m_contentType)) {
                result.append(JsonUtils.formatJson(m_body, prefix));
            }
            else {
                result.append(StringUtils.formatText(m_body, prefix));
            }
        }

        return result.toString();
    }
    
    private void parseStatusLine(String line)
    {
        m_code = -1;
        m_tag  = line.trim();

        int spacePos = m_tag.indexOf(' ');
        if (spacePos < 0) return;

        m_tag = m_tag.substring(spacePos + 1).trim();
        spacePos = m_tag.indexOf(' ');
        if (spacePos < 0) return;

        String item = m_tag.substring(0, spacePos);
        m_code = Integer.parseInt(item);

        m_tag = m_tag.substring(spacePos + 1).trim();
    }
}
