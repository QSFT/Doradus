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
import java.lang.reflect.Method;
import com.dell.doradus.testprocessor.common.*;

public class XReflectedSetter implements IXReflectedUnit
{
    private XMLReflector    m_xmlReflector;
    private String          m_name;
    private boolean         m_required;
    private boolean         m_trim;
    private boolean         m_expand;
    private Method          m_method;
    private Class<?>           m_setterType;
    private XReflectedType  m_xtype;

    public XReflectedSetter(XMLReflector xmlReflector, Method method, XReflectedType xtype)
    throws Exception
    {
        m_xmlReflector = xmlReflector;

        IXSetterReflector annotation = XAnnotations.getXSetterReflector(method);
        if (annotation == null) {
            String msg = "The \""
                    + xtype.getType().getName() + "." + method.getName()
                    + "\" method has no IXSetterReflector annotation";
            throw new Exception(msg);
        }

        Class<?>[] parmTypes = method.getParameterTypes();
        if (parmTypes.length != 1) {
            String msg = "The \""
                    + xtype.getType().getName() + "." + method.getName()
                    + "\" setter must have exactly one parameter";
            throw new Exception(msg);
        }

        m_name      = annotation.name();
        m_required  = annotation.required();
        m_trim      = annotation.trimValue();
        m_expand    = annotation.expand();
        m_method    = method;
        m_setterType = parmTypes[0];
        m_xtype     = xtype;
    }

    public String getName()             { return m_name;  }
    public void   setName(String value) { m_name = value; }
    public boolean isRequired() { return m_required; }

    public void enter() {
        // Do nothing
    }

    public void execute(Node xmlNode)
    throws Exception
    {
        try {
            Object arg;
            if (m_setterType == Node.class) {
                arg = xmlNode;
            } else {
                //String raw = xmlNode.getTextContent();
                String strArg = XmlUtils.getInnerXmlText(xmlNode);
                if (m_trim)   strArg = strArg.trim();
                if (m_expand) strArg = m_xmlReflector.definitions().expand(strArg);
                if (m_trim)   strArg = strArg.trim();
                arg = Utils.convert(strArg, m_setterType);
            }

            m_method.invoke(m_xtype.getInstance(), arg);
        }
        catch(Exception ex) {
            String msg = "Failed to execute the \""
                    + m_xtype.getType().getName() + "." + m_method.getName()
                    + "\" setter";
            throw new Exception(msg, ex);
        }
    }

    public void exit() {
        // Do nothing
    }
}
