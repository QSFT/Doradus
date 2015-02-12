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

import org.w3c.dom.Node;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class XReflectedType implements IXReflectedUnit
{
    private XMLReflector    m_xmlReflector;
    private String          m_name;
    private boolean         m_isLibrary;
    private boolean         m_isFinal;
    private Class<?>        m_type;
    private XReflectedType  m_outerXType;
    private Constructor<?>  m_defaultConstructor;
    private String          m_defaultConstructorHeader;
    private Object          m_instance;

    public XReflectedType(XMLReflector xmlReflector, Class<?> type, XReflectedType outerXType)
    throws Exception
    {
        m_xmlReflector = xmlReflector;

        IXTypeReflector annotation = XAnnotations.getXTypeReflector(type);
        if (annotation == null) {
            String msg = "The \"" + type.getName() + "\" class has no IXTypeReflector annotation";
            throw new Exception(msg);
        }

        m_name               = annotation.name();
        m_isLibrary          = annotation.isLibrary();
        m_isFinal            = annotation.isFinal();
        m_type               = type;
        m_outerXType         = outerXType;
        m_defaultConstructor = findDefaultConstructor();
        m_instance           = null;
    }

    public String  getName()             { return m_name;  }
    public void    setName(String value) { m_name = value; }
    public boolean isLibrary()           { return m_isLibrary; }
    public Class<?>   getType()          { return m_type; }
    public Object  getInstance()         { return m_instance; }

    public void enter()
    throws Exception
    {
        m_xmlReflector.dictionaryStack().openFrame();
        m_instance = createInstance();
        m_xmlReflector.commonData().put(m_name, m_instance);

        setXMLReflector();

        pushReflectedFields();
        pushReflectedSetters();
        pushReflectedNestedTypes();
    }

    public void execute(Node xmlNode)
    throws Exception
    {
        m_xmlReflector.processAttributes(xmlNode);
        if (!m_isFinal) {
            m_xmlReflector.processChildren(xmlNode);
        }
        if (m_instance instanceof IXTask)
            ((IXTask) m_instance).Run(xmlNode);
    }

    public void exit()
    throws Exception
    {
        m_xmlReflector.dictionaryStack().closeFrame();
    }

    private void pushReflectedFields()
    throws Exception
    {
        for (Field field : m_type.getFields())
        {
            IXFieldReflector annotation = XAnnotations.getXFieldReflector(field);
            if (annotation == null) continue;

            XReflectedField xfield = new XReflectedField(m_xmlReflector, field, this);
            m_xmlReflector.dictionaryStack().push(xfield);
        }
    }

    private void pushReflectedSetters()
    throws Exception
    {
        for (Method method : m_type.getMethods())
        {
            IXSetterReflector annotation = XAnnotations.getXSetterReflector(method);
            if (annotation == null) continue;

            XReflectedSetter xsetter = new XReflectedSetter(m_xmlReflector, method, this);
            m_xmlReflector.dictionaryStack().push(xsetter);
        }
    }

    private void pushReflectedNestedTypes()
    throws Exception
    {
        for (Class<?> nestedType : m_type.getClasses())
        {
            IXTypeReflector annotation = XAnnotations.getXTypeReflector(nestedType);
            if (annotation == null) continue;

            XReflectedType nestedXType = new XReflectedType(m_xmlReflector, nestedType, this);
            m_xmlReflector.dictionaryStack().push(nestedXType);
        }
    }

    private void setXMLReflector()
    throws Exception
    {
        for (Method method : m_type.getMethods()) {
            IXMLReflectorSetter annotation = XAnnotations.getXMLReflectorSetter(method);
            if (annotation != null) {
                method.invoke(m_instance, m_xmlReflector);
                break;
            }
        }
    }

    private Constructor<?> findDefaultConstructor()
    throws Exception
    {
        try {
            Class<?>[] parmTypes;

            //Constructor<?>[] constructors = m_type.getConstructors();

            if (m_outerXType == null ||
                m_outerXType.getInstance() == null ||
                Modifier.isStatic(m_type.getModifiers()))
            {
                parmTypes = new Class[0];
                m_defaultConstructorHeader = m_type.getName() + "()";
            }
            else {
                parmTypes = new Class[1];
                parmTypes[0] = m_outerXType.getType();
                m_defaultConstructorHeader = m_type.getName() + "(" + parmTypes[0].getName() + ")";
            }

            return m_type.getConstructor(parmTypes);
        }
        catch(Exception ex) {
            String msg = "Failed to find the (default) constructor \""
                    + m_defaultConstructorHeader + "\"";
            throw new Exception(msg, ex);
        }
    }

    private Object createInstance()
    throws Exception
    {
        try {
            if (m_defaultConstructor == null)
                return null;

            if (m_outerXType == null ||
                m_outerXType.getInstance() == null ||
                Modifier.isStatic(m_type.getModifiers()))
            {
                return m_defaultConstructor.newInstance();
            }

            return m_defaultConstructor.newInstance(m_outerXType.getInstance());
        }
        catch(Exception ex) {
            String msg = "Failed to create instance using the (default) \""
                    + m_defaultConstructorHeader + "\" constructor";
            throw new Exception(msg, ex);
        }
    }
}
