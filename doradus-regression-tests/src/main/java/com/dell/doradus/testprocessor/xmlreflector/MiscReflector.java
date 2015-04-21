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

import java.io.File;

import com.dell.doradus.testprocessor.common.*;
import org.w3c.dom.Node;

@IXTypeReflector(name="miscellaneous", isLibrary=true)
public class MiscReflector
{
    XMLReflector m_xmlReflector = null;

    @IXMLReflectorSetter
    public void setXMLReflector(XMLReflector xmlReflector) {
        m_xmlReflector = xmlReflector;
    }

    /*******************************************************************************
     *  DEFINE
     *  <define name="name" value="value" [trim="false|true"] />
     *  <define name="name" [trim="false|true"]> value </set>
     *******************************************************************************/
    @IXTypeReflector(name="define", isFinal = true)
    public class XDefine implements IXTask
    {
        @IXFieldReflector(name = "name", required = true, expand = false)
        public String m_name = null;

        @IXFieldReflector(name = "value")
        public String m_value = null;

        @IXFieldReflector(name = "trim")
        public boolean m_trim = false;

        public void Run(Node xmlNode)
        throws Exception
        {
            if (m_value == null) {
                m_value = XmlUtils.getInnerXmlText(xmlNode);
                m_value = m_xmlReflector.definitions().expand(m_value);
            }
            if (m_trim) {
                m_value = m_value.trim();
            }

            m_xmlReflector.definitions().setString(m_name, m_value);
        }
    }

    /*******************************************************************************
     *  UNDEFINE
     *  <undefine name="name" />
     *******************************************************************************/
    @IXTypeReflector(name="undefine", isFinal = true)
    public class XUndefine implements IXTask
    {
        @IXFieldReflector(name = "name", required = true, expand = false)
        public String m_name = null;

        public void Run(Node xmlNode) throws Exception {
            m_xmlReflector.definitions().remove(m_name);
        }
    }

    /*******************************************************************************
     *  ECHO
     *  <echo>text</echo>
     *******************************************************************************/
    @IXSetterReflector(name="echo")
    public void Echo(String text) {
        System.out.println(text);
    }

    /*******************************************************************************
     *  IMPORT
     *  <import>path</import>
     *******************************************************************************/
    @IXSetterReflector(name="import")
    public void executeScript(String path) throws Exception
    {
    	File saveCurrentScriptFile = m_xmlReflector.ScriptFile();
        try {
        	String absolutePath = FileUtils.combinePaths(m_xmlReflector.ScriptFileDir(), path);
            m_xmlReflector.processXmlFile(absolutePath);
        } catch(Exception ex) {
            String msg = "Failed to import the \"" + path + "\" script";
            throw new Exception(msg, ex);
        }
        finally {
        	m_xmlReflector.ScriptFile(saveCurrentScriptFile);
        }
    }

    /*******************************************************************************
     *  SLEEP
     *  <sleep [min="int"] [sec="int"] [msec="int"]/>
     *******************************************************************************/
    @IXTypeReflector(name="sleep")
    public class XSleep implements IXTask
    {
        @IXFieldReflector(name="min")
        public int m_minutes = 0;

        @IXFieldReflector(name="sec")
        public int m_seconds = 0;

        @IXFieldReflector(name="msec")
        public int m_milliseconds = 0;

        public void Run(Node xmlNode)
        {
            try {
                Thread.sleep((m_minutes*60 + m_seconds)*1000 + m_milliseconds);
            }
            catch(InterruptedException ex) {
                //Thread.currentThread().interrupt();
            }
        }
    }
}
