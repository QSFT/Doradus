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

package com.dell.doradus.testprocessor.common;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

@SuppressWarnings("restriction")
public class XmlUtils
{
    static public String unescapeXml(String src)
    throws Exception
    {
        if (src == null || src.trim().length() == 0)
            return src;

        StringBuilder result = new StringBuilder();
        int length = src.length();

        for (int i = 0; i < length; i++)
        {
            char ch = src.charAt(i);

            if (ch != '&') {
                result.append(ch);
                continue;
            }

            int pos = src.indexOf(";", i);
            if (pos < 0) {
                result.append(ch);
                continue;
            }

            if (src.charAt(i + 1) == '#') {
                String esc = "";
                try {
                    esc = src.substring(i + 2, pos);
                    int val = Integer.parseInt(src.substring(i + 2, pos), 16);
                    result.append((char) val);
                    i = pos;
                    continue;
                } catch(Exception ex) {
                    String msg = "Something wrong with hex code \"" + esc + "\"";
                    throw new Exception(msg, ex);
                }
            }

            String esc = src.substring(i, pos + 1);
            if (esc.equals("&amp;"))        result.append('&');
            else if (esc.equals("&lt;"))    result.append('<');
            else if (esc.equals("&gt;"))    result.append('>');
            else if (esc.equals("&quot;"))  result.append('"');
            else if (esc.equals("&apos;"))  result.append('\'');
            else {
                String msg = "Unknown escape \"" + esc + "\"";
                throw new Exception(msg);
            }
            i = pos;
        }

        return result.toString();
    }

    static public String escapeXml(String src)
    {
        if (src == null || src.trim().length() == 0)
            return src;

        StringBuilder result = new StringBuilder();
        int length = src.length();

        for (int i = 0; i < length; i++)
        {
            char ch = src.charAt(i);
            switch (ch) {
                case '&':  result.append("&amp;"); break;
                case '<':  result.append("&lt;"); break;
                case '>':  result.append("&gt;"); break;
                case '"':  result.append("&quot;"); break;
                case '\'': result.append("&apos;"); break;
                default:   result.append(ch); break;
            }
        }

        return result.toString();
    }

    static public String pathTo(Node xmlNode)
    {
        StringBuilder result = new StringBuilder();

        for (Node node = xmlNode; node != null; node = node.getParentNode()) {
            if (node.getNodeType() == Node.DOCUMENT_NODE)
                result.insert(0, node.getNodeName());
            else
                result.insert(0, " -> " + node.getNodeName());
        }

        return result.toString();
    }

    static public String getInnerXmlText(Node xmlNode)
    {
        StringBuilder result = new StringBuilder();

        Document xmlDocument = xmlNode.getOwnerDocument();
        DOMImplementation xmlDocumentImpl = xmlDocument.getImplementation();
        DOMImplementationLS lsImpl = (DOMImplementationLS) xmlDocumentImpl.getFeature("LS", "3.0");
        LSSerializer lsSerializer = lsImpl.createLSSerializer();

        NodeList childNodes = xmlNode.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            String childText = lsSerializer.writeToString(childNodes.item(i));
            int pos = childText.indexOf("?>");
            if (pos > -1) {
                childText = childText.substring(pos + 2);
            }
            result.append(childText);
        }

        return result.toString();
    }

    static public Node removeChild(Node xmlNode, String name, boolean ignoreCase)
    {
        Node removedChild = null;

        String key = name;
        if (ignoreCase) key = key.toLowerCase();

        NodeList childNodes = xmlNode.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++)
        {
            Node child = childNodes.item(i);
            String childName = child.getNodeName();
            if (ignoreCase) childName = childName.toLowerCase();

            if (childName.equals(key)) {
                removedChild = child;
                xmlNode.removeChild(child);
                break;
            }
        }
        return removedChild;
    }

    static public List<Node> removeChildren(Node xmlNode, String name, boolean ignoreCase)
    {
        List<Node> removedChildren = new ArrayList<Node>();

        while (true) {
            Node removedChild = removeChild(xmlNode, name, ignoreCase);
            if (removedChild == null) break;

            removedChildren.add(removedChild);
        }
        return removedChildren;
    }
	
	static public Document parseXml(String xmlText)
    throws Exception
	{
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		InputSource is = new InputSource(new StringReader(xmlText));
		return db.parse(is);
	}
	
	static public String formatXml(String xmlText, String prefix)
	throws Exception
	{
		if (xmlText == null)
			return xmlText;
		
		Document doc = parseXml(xmlText);
		
		OutputFormat format = new OutputFormat(doc);
        format.setLineWidth(120);
        format.setIndenting(true);
        format.setIndent(4);
        
        Writer out = new StringWriter();
        XMLSerializer serializer = new XMLSerializer(out, format);
        serializer.serialize(doc);
        xmlText = StringUtils.trim(out.toString(), " \r\n");

		if (xmlText.startsWith("<?xml")) {
			int ind = xmlText.indexOf("?>");
			if (ind > 1) {
				xmlText = xmlText.substring(ind + 2);
		        xmlText = StringUtils.trim(xmlText, " \r\n");
			}
		}
        
        return StringUtils.formatText(xmlText, prefix);
	}
}
