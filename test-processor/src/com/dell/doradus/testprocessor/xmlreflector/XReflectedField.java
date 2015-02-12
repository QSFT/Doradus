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
import java.lang.reflect.Field;
import com.dell.doradus.testprocessor.common.*;

public class XReflectedField implements IXReflectedUnit
{
    private XMLReflector    m_xmlReflector;
    private String          m_name;
    private boolean         m_required;
    private boolean         m_trim;
    private boolean         m_expand;
    private Field           m_field;
	private Class<?>           m_fieldType;
    private XReflectedType  m_xtype;

    public XReflectedField(XMLReflector xmlReflector, Field field, XReflectedType xtype)
    throws Exception
    {
        m_xmlReflector = xmlReflector;

        IXFieldReflector annotation = XAnnotations.getXFieldReflector(field);
        if (annotation == null) {
            String msg = "The \""
                    + xtype.getType().getName() + "." + field.getName()
                    + "\" field has no IXSetterReflector annotation";
            throw new Exception(msg);
        }

        m_name      = annotation.name();
        m_required  = annotation.required();
        m_trim      = annotation.trimValue();
        m_expand    = annotation.expand();
        m_field     = field;
        m_fieldType = field.getType();
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
            Object value;
            if (m_fieldType == Node.class) {
                value = xmlNode;
            } else {
                String strValue = XmlUtils.getInnerXmlText(xmlNode);
                if (m_trim)   strValue = strValue.trim();
                if (m_expand) strValue = m_xmlReflector.definitions().expand(strValue);
                if (m_trim)   strValue = strValue.trim();
                value = Utils.convert(strValue, m_fieldType);
            }

            m_field.set(m_xtype.getInstance(), value);
        }
        catch(Exception ex) {
            String msg = "Failed to set the \""
                    + m_xtype.getType().getName() + "." + m_field.getName()
                    + "\" field value";
            throw new Exception(msg, ex);
        }
    }

    public void exit() {
        // Do nothing
    }
}
