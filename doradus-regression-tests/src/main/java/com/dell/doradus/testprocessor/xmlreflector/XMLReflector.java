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

package com.dell.doradus.testprocessor.xmlreflector;

import com.dell.doradus.testprocessor.Log;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.dell.doradus.testprocessor.common.*;

public class XMLReflector
{
    private boolean             m_ignoreCase;
    private boolean             m_skipNotReflected;
    private XDictionaryStack    m_dictionaryStack;
    private XDefinitions        m_definitions;
    private Map<String, Object> m_commonData;
    private File				m_scriptFile;

    public XMLReflector()
    {
        m_skipNotReflected  = false;
        m_ignoreCase        = false;
        m_dictionaryStack   = new XDictionaryStack();
        m_definitions       = new XDefinitions();
        m_commonData        = new HashMap<String, Object>();
        m_scriptFile		= null;
    }

    public void ignoreCase(boolean value) {
        m_ignoreCase = value;
        m_dictionaryStack.ignoreCase(value);
    }
    public boolean ignoreCase() {
        return m_ignoreCase;
    }
    public void skipNotReflected(boolean value) {
        m_skipNotReflected = value;
    }
    public boolean skipNotReflected() {
        return m_skipNotReflected;
    }
    public XDictionaryStack dictionaryStack() {
        return m_dictionaryStack;
    }
    public XDefinitions definitions() {
        return m_definitions;
    }
    public Map<String, Object> commonData() {
        return m_commonData;
    }
    public void ScriptFile(File value) {
    	m_scriptFile = value;
    }
    public File ScriptFile() {
    	return m_scriptFile;
    }
    public String ScriptFilePath() {
    	if (m_scriptFile == null)
    		return null;
    	return m_scriptFile.getAbsolutePath();
    }
    public String ScriptFileDir() {
    	
    	if (m_scriptFile == null)
    		return null;
    	Path path = Paths.get(m_scriptFile.getAbsolutePath());
    	return path.getParent().toString();
    }

    public void include(Class<?> type)
    throws Exception
    {
        IXTypeReflector annotation = XAnnotations.getXTypeReflector(type);
        if (annotation == null)  return;

        Log.println("XMLReflector: including \"" + type.getName() + "\"");

        XReflectedType xtype = new XReflectedType(this, type, null);

        if (annotation.isLibrary())
            xtype.enter();
        else
            m_dictionaryStack.push(xtype);
    }

    public void includeJar(String pathToJar)
    throws Exception
    {
        try {
            JarFile jarFile = new JarFile(pathToJar);
            Enumeration<?> e = jarFile.entries();
            URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
            URLClassLoader classLoader = URLClassLoader.newInstance(urls);

            List<String> classNames = new ArrayList<String>();

            while (e.hasMoreElements())
            {
                JarEntry jarEntry = (JarEntry) e.nextElement();
                if (jarEntry.isDirectory()) continue;

                String entryName = jarEntry.getName();
                if (!entryName.endsWith(".class") || entryName.indexOf('$') > -1) continue;

                // remove ".class" at the end
                String className = entryName.substring(0, entryName.length() - 6);
                className = className.replace('/', '.');

                classNames.add(className);
            }

            while (!classNames.isEmpty())
            {
                String shortestName = classNames.get(0);
                int shortestNameIndex = 0;

                for (int i = 1; i < classNames.size(); i++)
                {
                    String name = classNames.get(i);
                    if (name.length() < shortestName.length()) {
                        shortestName = name;
                        shortestNameIndex = i;
                    }
                }

                Class<?> type = classLoader.loadClass(shortestName);
                include(type);

                classNames.remove(shortestNameIndex);
            }
            jarFile.close();
        }
        catch(Exception ex) {
            String msg = "Failed to include .jar file \"" + pathToJar + "\"";
            throw new Exception(msg, ex);
        }
    }

    public void processXmlFile(String xmlFilePath)
    throws Exception
    {
    	ScriptFile(new File(xmlFilePath));
        InputStream xmlInputString = new FileInputStream(ScriptFile());
        process(xmlInputString);
    }

    public void processXmlText(String xmlText)
    throws Exception
    {
        byte[] xmlBytesArray = xmlText.getBytes(Charset.forName("UTF-8"));
        InputStream xmlInputString = new ByteArrayInputStream(xmlBytesArray);
        process(xmlInputString);
    }

    public void process(InputStream xmlInputStream)
    throws Exception
    {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setValidating(false);
        DocumentBuilder builder = f.newDocumentBuilder();
        Document xmlDoc = builder.parse(xmlInputStream);
        process(xmlDoc);
    }

    public void process(Document xmlDoc)
    throws Exception
    {
        Node xmlRoot = xmlDoc.getDocumentElement();
        process(xmlRoot);
    }

    public void process(Node xmlNode)
    throws Exception
    {
        processAttributes(xmlNode);
        processChildren(xmlNode);
    }

    public void processAttributes(Node xmlNode)
    throws Exception
    {
        String attrName = "";

        try {
            NamedNodeMap xmlAttrs = xmlNode.getAttributes();
            for (int i = 0; i < xmlAttrs.getLength(); i++)
            {
                Node xmlAttr = xmlAttrs.item(i);
                attrName = xmlAttr.getNodeName();

                IXReflectedUnit xunit = m_dictionaryStack.find(attrName);
                if (xunit == null) {
                    if (m_skipNotReflected) continue;
                    throw new Exception("Attribute \"" + attrName + "\" is not found");
                }

                xunit.enter();
                xunit.execute(xmlAttr);
                xunit.exit();
            }
        }
        catch(Exception ex) {
            String msg = "Xml path: " + XmlUtils.pathTo(xmlNode) + " -> " + attrName;
            throw new Exception(msg, ex);
        }
    }

    public void processChildren(Node xmlNode)
    throws Exception
    {
        String childName = "";

        try {
            NodeList xmlChildNodes = xmlNode.getChildNodes();
            for (int i = 0; i < xmlChildNodes.getLength(); i++)
            {
                Node xmlChild = xmlChildNodes.item(i);
                if (xmlChild.getNodeType() != Node.ELEMENT_NODE) continue;

                childName = xmlChild.getNodeName();

                IXReflectedUnit xunit = m_dictionaryStack.find(childName);
                if (xunit == null) {
                    if (m_skipNotReflected) continue;
                    throw new Exception("Node \"" + childName + "\" is not found");
                }

                xunit.enter();
                xunit.execute(xmlChild);
                xunit.exit();
            }
        }
        catch(Exception ex) {
            String msg = "Xml path: " + XmlUtils.pathTo(xmlNode) + " -> " + childName;
            throw new Exception(msg, ex);
        }
    }
}
