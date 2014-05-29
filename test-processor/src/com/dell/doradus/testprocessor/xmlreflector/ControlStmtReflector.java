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

import com.dell.doradus.testprocessor.common.*;

import java.util.List;

@IXTypeReflector(name="control.statements", isLibrary=true)
public class ControlStmtReflector
{
    XMLReflector m_xmlReflector = null;

    @IXMLReflectorSetter
    public void setXMLReflector(XMLReflector xmlReflector) {
        m_xmlReflector = xmlReflector;
    }

    /*******************************************************************************
     *  FOR Statement
     *  <for index="ind" from="int" to="int" [step="int"]>
     *      ... ${ind} ...
     *  </for>
     *******************************************************************************/
    @IXTypeReflector(name="for", isFinal=true)
    public class XForStmt implements IXTask
    {
        @IXFieldReflector(name="index", required=true)
        public String m_index;

        @IXFieldReflector(name="from", required=true)
        public int m_from;

        @IXFieldReflector(name="to", required=true)
        public int m_to;

        @IXFieldReflector(name="step", required=false)
        private int m_step = 1;

        public void Run(Node xmlNode)
        throws Exception
        {
            try {
                for (int i = m_from; i <= m_to; i += m_step) {
                    m_xmlReflector.definitions().setInteger(m_index, i);
                    m_xmlReflector.process(xmlNode);
                }
            } catch(Exception ex) {
                throw ex;
            }
            finally {
                m_xmlReflector.definitions().remove(m_index);
            }
        }
    }

    /*******************************************************************************
     *  IF Statements
     *******************************************************************************/
    private void IFExecute(boolean condition, Node xmlNode)
    throws Exception
    {
        if (condition) {
            m_xmlReflector.process(xmlNode);
        }
    }

    public class TwoIntArgs
    {
        @IXFieldReflector(name="arg1", required=true)
        public int m_arg1;

        @IXFieldReflector(name="arg2", required=true)
        public int m_arg2;
    }

    /*******************************************************************************
     *  <IF.LT arg1="int" arg2="int">
     *      ...
     *  </IF.LT>
     *******************************************************************************/
    @IXTypeReflector(name="IF.LT", isFinal = true)
    public class IFLTStmt extends TwoIntArgs implements IXTask
    {
        public void Run(Node xmlNode) throws Exception {
            IFExecute(m_arg1 < m_arg2, xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.LE arg1="int" arg2="int">
     *      ...
     *  </IF.LE>
     *******************************************************************************/
    @IXTypeReflector(name="IF.LE", isFinal = true)
    public class IFLEStmt extends TwoIntArgs implements IXTask
    {
        public void Run(Node xmlNode) throws Exception {
            IFExecute(m_arg1 <= m_arg2, xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.EQ arg1="int" arg2="int">
     *      ...
     *  </IF.EQ>
     *******************************************************************************/
    @IXTypeReflector(name="IF.EQ", isFinal = true)
    public class IFEQStmt extends TwoIntArgs implements IXTask
    {
        public void Run(Node xmlNode) throws Exception {
            IFExecute(m_arg1 == m_arg2, xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.NE arg1="int" arg2="int">
     *      ...
     *  </IF.NE>
     *******************************************************************************/
    @IXTypeReflector(name="IF.NE", isFinal = true)
    public class IFNEStmt extends TwoIntArgs implements IXTask
    {
        public void Run(Node xmlNode) throws Exception {
            IFExecute(m_arg1 != m_arg2, xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.GE arg1="int" arg2="int">
     *      ...
     *  </IF.GE>
     *******************************************************************************/
    @IXTypeReflector(name="IF.GE", isFinal = true)
    public class IFGEStmt extends TwoIntArgs implements IXTask
    {
        public void Run(Node xmlNode) throws Exception {
            IFExecute(m_arg1 >= m_arg2, xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.GT arg1="int" arg2="int">
     *      ...
     *  </IF.GT>
     *******************************************************************************/
    @IXTypeReflector(name="IF.GT", isFinal = true)
    public class IFGTStmt extends TwoIntArgs implements IXTask
    {
        public void Run(Node xmlNode) throws Exception {
            IFExecute(m_arg1 > m_arg2, xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.DEF name="name">
     *      ...
     *  </IF.DEF>
     *******************************************************************************/
    @IXTypeReflector(name="IF.DEF", isFinal = true)
    public class IFDEFStmt implements IXTask
    {
        @IXFieldReflector(name = "name", required = true)
        public String m_name = null;

        public void Run(Node xmlNode) throws Exception  {
            IFExecute(m_xmlReflector.definitions().isDefined(m_name), xmlNode);
        }
    }
    /*******************************************************************************
     *  <IF.UNDEF name="name">
     *      ...
     *  </IF.UNDDEF>
     *******************************************************************************/
    @IXTypeReflector(name="IF.UNDEF", isFinal = true)
    public class IFUNDEFStmt implements IXTask
    {
        @IXFieldReflector(name = "name", required = true)
        public String m_name = null;

        public void Run(Node xmlNode) throws Exception {
            IFExecute(!m_xmlReflector.definitions().isDefined(m_name), xmlNode);
        }
    }

    /*******************************************************************************
     *  TRY-CATCH Statement
     *******************************************************************************/
    @IXTypeReflector(name="try", isFinal=true)
    public class XTry implements IXTask
    {
        public void Run(Node xmlNode)
        throws Exception
        {
            List<Node> catchNodes = XmlUtils.removeChildren(xmlNode, "catch", m_xmlReflector.ignoreCase());
            if (catchNodes.size() > 1) {
                String msg = "Try statement contains multiple catch-statements";
                throw new Exception(msg);
            }

            try {
                m_xmlReflector.process(xmlNode);
            }
            catch(Exception ex) {
                if (catchNodes.size() == 1) {
                    m_xmlReflector.definitions().setString("exception", Utils.unwind(ex));
                    m_xmlReflector.process(catchNodes.get(0));
                    m_xmlReflector.definitions().remove("exception");
                }
            }
        }
    }
}
