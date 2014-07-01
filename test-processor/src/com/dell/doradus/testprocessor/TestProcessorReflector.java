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

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.w3c.dom.Node;
import com.dell.doradus.testprocessor.common.*;
import com.dell.doradus.testprocessor.xmlreflector.*;

@IXTypeReflector(name="xdoradus", isLibrary = true)
public class TestProcessorReflector
{
    /*******************************************************************************
     *  Reserved variables
     *******************************************************************************/
    static public final String VAR_DORADUS_HOST    = "doradus.host";       // String
    static public final String VAR_DORADUS_PORT    = "doradus.port";       // int
    static public final String VAR_OUTPUT_ENABLED  = "output.enabled";     // boolean
    static public final String VAR_RESPONSE_FORMAT = "response.format";    // String

    /*******************************************************************************
     *  Private members
     *******************************************************************************/
    XMLReflector  m_xmlReflector;
    XDefinitions  m_definitions;
    RestClient    m_restClient;
    Writer        m_resultWriter;

    /*******************************************************************************
     *  This function is called by XMLReflector after instantiating this library.
     *  xmlReflector is the instance of XMLReflector which calls this function.
     *******************************************************************************/
    @IXMLReflectorSetter
    public void setXMLReflector(XMLReflector xmlReflector)
    {
        m_xmlReflector = xmlReflector;
        m_definitions  = xmlReflector.definitions();
    }

    /*******************************************************************************
     *  This function is called by TestProcessor after instantiating this library.
     *  The resultWriter is to be used for writing results of the test script.
     *******************************************************************************/
    public void setResultWriter(Writer resultWriter)
    {
        m_resultWriter = resultWriter;
    }

    private void writeToResultFile(String text)
    throws Exception
    {
        if (m_resultWriter == null || !outputEnabled())
            return;

        m_resultWriter.write(text);
        m_resultWriter.flush();
    }

    private void writeLineToResultFile(String text)
    throws Exception
    {
        writeToResultFile(text + Utils.EOL);
    }

    /*******************************************************************************
     *  <enable-output/>
     *  Equivalent to <define name="output.enabled" value="true"/>
     *******************************************************************************/
    @IXSetterReflector(name="enable-output")
    public void EnableOutput(String ignore) throws Exception {
        m_definitions.setBoolean(VAR_OUTPUT_ENABLED, true);
    }
    /*******************************************************************************
     *  <disable-output/>
     *  Equivalent to <define name="output.enabled" value="false"/>
     *******************************************************************************/
    @IXSetterReflector(name="disable-output")
    public void DisableOutput(String ignore) throws Exception {
        m_definitions.setBoolean(VAR_OUTPUT_ENABLED, false);
    }

    private boolean outputEnabled() {
        try { return m_definitions.getBoolean(VAR_OUTPUT_ENABLED); }
        catch(Exception ex) { return false; }
    }

    /*******************************************************************************
     *  <echo>...</echo>
     *******************************************************************************/
    @IXSetterReflector(name="echo")
    public void Echo(String text)
    throws Exception
    {
        writeLineToResultFile(XmlUtils.unescapeXml(text));
    }
    
    /*******************************************************************************
     *  <json-from-xml name="..." value="..." />   
     *  <json-from-xml name="...">
     *      ... xml text ...
     *  </json-from-xml>
     *******************************************************************************/
    @IXTypeReflector(name="json-from-xml")
    public class JsonFromXml implements IXTask
    {
        @IXFieldReflector(name="name", required = true)
        public String jsonVariableName = null;
        
        @IXFieldReflector(name="value", required = false)
        public String xmlText = null;
        
        public void Run(Node xmlNode)
        throws Exception
        {
        	if (xmlText == null)
        		xmlText = XmlUtils.getInnerXmlText(xmlNode);
        	
        	String jsonText = JsonUtils.convertXmlToJson(xmlText);
            m_definitions.setString(jsonVariableName, jsonText);
        }
    }
    
    /*******************************************************************************
     *  <connect [host="..."] [port="..."] />
     *******************************************************************************/
    @IXTypeReflector(name="connect")
    public class Connect implements IXTask
    {
        @IXFieldReflector(name="host")
        public String host = null;

        @IXFieldReflector(name="port")
        public int port = -1;

        public void Run(Node xmlNode)
        throws Exception
        {
            if (host == null) {
                try { host = m_definitions.getString(VAR_DORADUS_HOST); }
                catch(Exception ex) {
                    String msg = "Host is not defined";
                    throw new Exception(msg, ex);
                }
            }
            if (port == -1) {
                try { port = m_definitions.getInteger(VAR_DORADUS_PORT); }
                catch(Exception ex) {
                    String msg = "Host is not defined";
                    throw new Exception(msg, ex);
                }
            }

            m_restClient = new RestClient();
            m_restClient.connect(host, port);
        }
    }
    
    /*******************************************************************************
     *  <disconnect/>
     *******************************************************************************/
    @IXTypeReflector(name="disconnect")
    public class Disconnect implements IXTask
    {
        public void Run(Node xmlNode)
        throws Exception
        {
            if (m_restClient != null)
                m_restClient.disconnect();
        }
    }

    /*******************************************************************************
     *  Execute Request
     *******************************************************************************/
    private void executeRequest(
        String httpMethod,
        String uri,
        String body,
        String bodyFormat
    )
    throws Exception
    {
        if (m_restClient == null) {
            String msg = "No connection to Doradus server (possibly <connect> statement is missed)";
            throw new Exception(msg);
        }

        if (uri.indexOf('/') != 0) uri = "/" + uri;

        RestRequest request = new RestRequest();
        request.setRequestFormat(bodyFormat);
        request.setRest(httpMethod + " " + uri);
        request.setBody(body);

        writeLineToResultFile("*** Request");
        writeLineToResultFile(request.toString("    "));

        String value = m_definitions.getString(VAR_RESPONSE_FORMAT);
        List<String> responseFormats = StringUtils.split(value, '|', true);

        for (int i = 0; i < responseFormats.size(); i++)
        {
            request.setResponseFormat(responseFormats.get(i));
            m_restClient.sendRequest(request);
            RestResponse response = m_restClient.readResponse();

            writeLineToResultFile("*** Response: " + response.getContentType());
            writeLineToResultFile(response.toString("    "));
            
            if (response.getContentLength() < 1)
            	break;
            
            if (!responseFormats.contains(response.getContentType()))
            	break;
        }
        
        writeToResultFile(Utils.EOL);
    }

    /*******************************************************************************
     *  
     *******************************************************************************/
    public class XPath
    {
        private String m_path = null;

        public String getPath() {
        	return m_path;
        }
        
        @IXSetterReflector(name = "path", required = true)
        public void setPath(String path) throws Exception {
            m_path = XmlUtils.unescapeXml(path);
        }
    }

    /*******************************************************************************
     *  
     *******************************************************************************/
    public class XPathAndBody extends XPath
    {
        private String m_body = null;
        private String m_bodyFormat = null;

        public String getBody() {
        	return m_body;
        }
        public String getBodyFormat() {
        	return m_bodyFormat;
        }
        
        @IXSetterReflector(name = "xml")
        public void setXmlBody(String value) {
            m_body = value;
            m_bodyFormat = "xml";
        }

        @IXSetterReflector(name = "json")
        public void setJsonBody(String value) {
            m_body = value;
            m_bodyFormat = "json";
        }

        @IXSetterReflector(name = "xml-file")
        public void readXmlBodyFromFile(String value) throws Exception {
        	String absolutePath = FileUtils.combinePaths(m_xmlReflector.ScriptFileDir(), value);
        	m_body = StringUtils.trim(new String(Files.readAllBytes(Paths.get(absolutePath))), " \r\n\\t");
            m_bodyFormat = "xml";
        }

        @IXSetterReflector(name = "json-file")
        public void readJsonBodyFromFile(String value) throws Exception {
        	String absolutePath = FileUtils.combinePaths(m_xmlReflector.ScriptFileDir(), value);
        	m_body = StringUtils.trim(new String(Files.readAllBytes(Paths.get(absolutePath))), " \r\n\\t");
            m_bodyFormat = "json";
        }
    }

    /*******************************************************************************
     *  <AGGREGATE path="..."
     *    m="..."        - comma-separated list of metric expression(s)
     *    q="..."        - query expression
     *    f="..."        - grouping parameter
     *    cf="..."       - composite grouping parameter
     *    pair="..."     - comma-separated pair of link paths
     *    shards="..."   - comma-separated list of shards
     *    range="..."    - range of shards: shard-from[,shard-to]
     *    xshards="..."  - 
     *    xrange="..."   - 
     *  />
     *******************************************************************************/
    @IXTypeReflector(name = "AGGREGATE")
    public class XAggregate extends XPath implements IXTask
    {
        private String m_metric = null;
        @IXSetterReflector(name = "m", required = true)
        public void setMetric(String metric) throws Exception {
        	m_metric = XmlUtils.unescapeXml(metric);
        	m_metric = Utils.urlEncode(m_metric);
        }

        private String m_queryExpr = null;
        @IXSetterReflector(name = "q")
        public void setQueryExpr(String queryExpr) throws Exception {
            m_queryExpr = XmlUtils.unescapeXml(queryExpr);
            m_queryExpr = Utils.urlEncode(m_queryExpr);
        }

        private String m_grouping = null;
        @IXSetterReflector(name = "f")
        public void setGrouping(String grouping) throws Exception {
        	m_grouping = XmlUtils.unescapeXml(grouping);
        	m_grouping = Utils.urlEncode(m_grouping);
        }

        private String m_compositeGrouping = null;
        @IXSetterReflector(name = "cf")
        public void setCompositeGrouping(String compositeGrouping) throws Exception {
        	m_compositeGrouping = XmlUtils.unescapeXml(compositeGrouping);
        	m_compositeGrouping = Utils.urlEncode(m_compositeGrouping);
        }
        
        private String m_pair = null;
        @IXSetterReflector(name = "pair")
        public void setPair(String pair) throws Exception {
        	m_pair = XmlUtils.unescapeXml(pair);
        	m_pair = Utils.urlEncode(m_pair);
        }
        
        private String m_shards = null;
        @IXSetterReflector(name = "shards")
        public void setShards(String shards) throws Exception {
        	m_shards = XmlUtils.unescapeXml(shards);
        	m_shards = Utils.urlEncode(m_shards);
        }

        private String m_range = null;
        @IXSetterReflector(name = "range")
        public void setRange(String range) throws Exception {
        	m_range = XmlUtils.unescapeXml(range);
        	m_range = Utils.urlEncode(m_range);
        }
        
        private String m_xshards = null;
        @IXSetterReflector(name = "xshards")
        public void setXShards(String xshards) throws Exception {
        	m_xshards = XmlUtils.unescapeXml(xshards);
        	m_xshards = Utils.urlEncode(m_xshards);
        }

        private String m_xrange = null;
        @IXSetterReflector(name = "xrange")
        public void setXRange(String xrange) throws Exception {
        	m_xrange = XmlUtils.unescapeXml(xrange);
        	m_xrange = Utils.urlEncode(m_xrange);
        }

        public void Run(Node xmlNode)
        throws Exception
        {
            StringBuilder uri = new StringBuilder();

            uri.append(getPath() + "/_aggregate?m=" + m_metric);
            if (m_queryExpr != null)         uri.append("&q=" + m_queryExpr);
            if (m_grouping != null)          uri.append("&f=" + m_grouping);
            if (m_compositeGrouping != null) uri.append("&cf=" + m_compositeGrouping);
            if (m_pair != null)              uri.append("&pair=" + m_pair);
            if (m_shards != null)            uri.append("&shards=" + m_shards);
            if (m_range != null)             uri.append("&range=" + m_range);
            if (m_xshards != null)           uri.append("&xshards=" + m_xshards);
            if (m_xrange != null)            uri.append("&xrange=" + m_xrange);

            executeRequest("GET", uri.toString(), null, null);
        }
    }

    /*******************************************************************************
     *  <DELETE path="..."/>
     *******************************************************************************/
    @IXTypeReflector(name = "DELETE")
    public class XDelete extends XPathAndBody implements IXTask
    {
        public void Run(Node xmlNode)
        throws Exception
        {
            executeRequest("DELETE", getPath(), getBody(), getBodyFormat());
        }
    }

    /*******************************************************************************
     *  <DUPLICATES path="..." [range="...'] />
     *******************************************************************************/
    @IXTypeReflector(name = "DUPLICATES")
    public class XDuplicates extends XPath implements IXTask
    {
        private String m_range = null;

        @IXSetterReflector(name = "range")
        public void setRange(String range) throws Exception {
            m_range = XmlUtils.unescapeXml(range);
            m_range = Utils.urlEncode(m_range);
        }

        public void Run(Node xmlNode)
        throws Exception
        {
            StringBuilder uri = new StringBuilder();

            uri.append(getPath() + "/_duplicates");
            if (m_range != null) uri.append("?range=" + m_range);

            executeRequest("GET", uri.toString(), null, null);
        }
    }

    /*******************************************************************************
     *  <GET path="..."/>
     *  <GET path="..."><XML>body</XML></PUT>
     *  <GET path="..." xml="body" />
     *  <GET path="..." xml-file="relative-path" />
     *  <GET path="..."><JSON>body</JSON></PUT>
     *  <GET path="..." json="body" />
     *  <GET path="..." json-file="relative-path" />
     *******************************************************************************/
    @IXTypeReflector(name = "GET")
    public class XGet extends XPathAndBody implements IXTask
    {
        public void Run(Node xmlNode)
        throws Exception
        {
            executeRequest("GET", getPath(), getBody(), getBodyFormat());
        }
    }

    /*******************************************************************************
     *  <POST path="..."/>
     *  <POST path="..."><XML>body</XML></POST>
     *  <POST path="..." xml="body" />
     *  <POST path="..."><JSON>body</JSON></POST>
     *  <POST path="..." json="body" />
     *******************************************************************************/
    @IXTypeReflector(name = "POST")
    public class XPost extends XPathAndBody implements IXTask
    {
        private Boolean m_overwrite = null;
        @IXSetterReflector(name = "overwrite")
        public void setOverwrite(boolean overwrite) {
            m_overwrite = overwrite;
        }

        public void Run(Node xmlNode)
        throws Exception
        {
            StringBuilder uri = new StringBuilder();
            uri.append(getPath());

            String delim = "?";

            if (m_overwrite != null) {
                uri.append(delim + "overwrite=" + m_overwrite);
                delim = "&";
            }

            executeRequest("POST", uri.toString(), getBody(), getBodyFormat());
        }
    }

    /*******************************************************************************
     *  <PUT path="..."/>
     *  <PUT path="..."><XML>body</XML></PUT>
     *  <PUT path="..." xml="body" />
     *  <PUT path="..." xml-file="relative-path" />
     *  <PUT path="..."><JSON>body</JSON></PUT>
     *  <PUT path="..." json="body" />
     *  <PUT path="..." json-file="relative-path" />
     *******************************************************************************/
    @IXTypeReflector(name = "PUT")
    public class XPut extends XPathAndBody implements IXTask
    {
        private Boolean m_overwrite = null;
        @IXSetterReflector(name = "overwrite")
        public void setOverwrite(boolean overwrite) {
            m_overwrite = overwrite;
        }

        public void Run(Node xmlNode)
        throws Exception
        {
            StringBuilder uri = new StringBuilder();
            uri.append(getPath());

            String delim = "?";

            if (m_overwrite != null) {
                uri.append(delim + "overwrite=" + m_overwrite);
                delim = "&";
            }

            executeRequest("PUT", uri.toString(), getBody(), getBodyFormat());
        }
    }

    /*******************************************************************************
     *  <QUERY path="..."
     *    q="..."        - query expression
     *    f="..."        - comma-separated list of fields to return
     *    s="..."        - number of objects to return
     *    o="..."        - order the results by the given scalar field name
     *    k="..."        - skip the given number of objects
     *    e="..."        - continue-at token
     *    g="..."        - continue-after token
     *    shards="..."   - comma-separated list of shards
     *    range="..."    - range of shards: shard-from[,shard-to]
     *  />
     *******************************************************************************/
    @IXTypeReflector(name = "QUERY")
    public class XQuery extends XPath implements IXTask
    {
        private String m_queryExpr = null;
        @IXSetterReflector(name = "q")
        public void setQueryExpr(String queryExpr) throws Exception {
            m_queryExpr = XmlUtils.unescapeXml(queryExpr);
            m_queryExpr = Utils.urlEncode(m_queryExpr);
        }

        private String m_fields = null;
        @IXSetterReflector(name = "f")
        public void setFields(String fields) throws Exception {
        	m_fields = XmlUtils.unescapeXml(fields);
        	m_fields = Utils.urlEncode(m_fields);
        }

        private String m_size = null;
        @IXSetterReflector(name = "s")
        public void setSize(String size) throws Exception {
            m_size = XmlUtils.unescapeXml(size);
            m_size = Utils.urlEncode(m_size);
        }

        private String m_order = null;
        @IXSetterReflector(name = "o")
        public void setOrder(String order) throws Exception {
        	m_order = XmlUtils.unescapeXml(order);
        	m_order = Utils.urlEncode(m_order);
        }

        private String m_skip = null;
        @IXSetterReflector(name = "k")
        public void setSkip(String skip) throws Exception {
        	m_skip = XmlUtils.unescapeXml(skip);
        	m_skip = Utils.urlEncode(m_skip);
        }
        
        private String m_continueAt = null;
        @IXSetterReflector(name = "e")
        public void setContinueAt(String continueAt) throws Exception {
        	m_continueAt = XmlUtils.unescapeXml(continueAt);
        	m_continueAt = Utils.urlEncode(m_continueAt);
        }

        private String m_continueAfter = null;
        @IXSetterReflector(name = "g")
        public void setContinueAfter(String continueAfter) throws Exception {
        	m_continueAfter = XmlUtils.unescapeXml(continueAfter);
        	m_continueAfter = Utils.urlEncode(m_continueAfter);
        }

        private String m_shards = null;
        @IXSetterReflector(name = "shards")
        public void setShards(String shards) throws Exception {
        	m_shards = XmlUtils.unescapeXml(shards);
        	m_shards = Utils.urlEncode(m_shards);
        }

        private String m_range = null;
        @IXSetterReflector(name = "range")
        public void setRange(String range) throws Exception {
        	m_range = XmlUtils.unescapeXml(range);
        	m_range = Utils.urlEncode(m_range);
        }

        public void Run(Node xmlNode)
        throws Exception
        {
            StringBuilder uri = new StringBuilder();
            uri.append(getPath() + "/_query");
            
            String delim = "?";
            
            if (m_queryExpr != null) {
            	uri.append(delim + "q=" + m_queryExpr);
            	delim = "&";
            }
            if (m_fields != null) {
            	uri.append(delim + "f=" + m_fields);
            	delim = "&";
            }
            if (m_size != null) {
            	uri.append(delim + "s=" + m_size);
            	delim = "&";
            }
            if (m_order != null) {
            	uri.append(delim + "o=" + m_order);
            	delim = "&";
            }
            if (m_skip != null) {
            	uri.append(delim + "k=" + m_skip);
            	delim = "&";
            }
            if (m_continueAt != null) {
            	uri.append(delim + "e=" + m_continueAt);
            	delim = "&";
            }
            if (m_continueAfter != null) {
            	uri.append(delim + "g=" + m_continueAfter);
            	delim = "&";
            }
            if (m_shards != null) {
            	uri.append(delim + "shards=" + m_shards);
            	delim = "&";
            }
            if (m_range != null) {
            	uri.append(delim + "range=" + m_range);
            	delim = "&";
            }

            executeRequest("GET", uri.toString(), null, null);
        }
    }
}
