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

package com.dell.doradus.testprocessor.config;

import com.dell.doradus.testprocessor.Config;
import com.dell.doradus.testprocessor.TestSuiteInfo;
import com.dell.doradus.testprocessor.common.FileUtils;
import com.dell.doradus.testprocessor.common.Utils;
import com.dell.doradus.testprocessor.xmlreflector.*;
import org.w3c.dom.Node;

@IXTypeReflector(name="config", isLibrary=true)
public class ConfigReflector
{
    @IXSetterReflector(name="log")
    public void setLogFile(String value) {
        Config.logFilePath = value;
    }

    @IXSetterReflector(name="report")
    public void setReportFile(String value) {
        Config.reportFilePath = value;
    }

    @IXSetterReflector(name="doradus-host")
    public void setDoradusHost(String value) {
        Config.doradusHost = value;
    }

    @IXSetterReflector(name="doradus-port")
    public void setDoradusPort(int value) {
        Config.doradusPort = value;
    }

    @IXSetterReflector(name="output-enabled")
    public void setOutputEnabled(boolean value) {
        Config.outputEnabled = value;
    }

    @IXSetterReflector(name="response-format")
    public void setResponseFormat(String value) {
        Config.responseFormat = value;
    }

    /*  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *  TEST-SUITE
     *  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
    @IXTypeReflector(name="test-suite")
    public class XTestSuite implements IXTask
    {
        private TestSuiteSection m_testSuiteSection;

        public XTestSuite() {
            m_testSuiteSection = new TestSuiteSection();
            m_testSuiteSection.start();
        }
        @IXSetterReflector(name="root")
        public void setRootDirectory(String value) {
            m_testSuiteSection.setRootPath(value);
        }

        public void Run(Node xmlNode)
        throws Exception
        {
            m_testSuiteSection.finish();
            Config.testSuiteInfo = new TestSuiteInfo();
            m_testSuiteSection.applyTo(Config.testSuiteInfo);
        }

        /*  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
         *  INCLUDE SECTION
         *  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
        @IXTypeReflector(name="include")
        public class XInclude implements IXTask
        {
            private IncludeSection m_includeSection;

            public XInclude() {
                m_includeSection = new IncludeSection();
                m_includeSection.start(m_testSuiteSection.getRootPath());
            }

            public void Run(Node xmlNode)
            throws Exception
            {
                m_includeSection.finish();
                m_testSuiteSection.setIncludeSection(m_includeSection);
            }

            @IXTypeReflector(name="test")
            public class XTest
            {
                @IXSetterReflector(name="name")
                public void setName(String value) {
                    m_includeSection.addTest(value);
                }
            }

            @IXTypeReflector(name="dir")
            public class XDir implements IXTask
            {
                @IXSetterReflector(name="path")
                public void setPath(String value) {
                    m_includeSection.openDir(value);
                }

                public void Run(Node xmlNode)
                throws Exception {
                    m_includeSection.closeDir();
                }
            }
        }

        /*  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
         *  EXCLUDE SECTION
         *  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
        @IXTypeReflector(name="exclude")
        public class XExclude implements IXTask
        {
            private ExcludeSection m_excludeSection;

            public XExclude() {
                m_excludeSection = new ExcludeSection();
                m_excludeSection.start(m_testSuiteSection.getRootPath());
            }

            public void Run(Node xmlNode)
            throws Exception
            {
                m_excludeSection.finish();
                m_testSuiteSection.setExcludeSection(m_excludeSection);
            }

            @IXTypeReflector(name="test")
            public class XTest implements IXTask
            {
                private String m_name;
                @IXSetterReflector(name="name")
                public void setName(String value) {
                    m_name = value;
                }

                private String m_reason = "";
                @IXSetterReflector(name="reason")
                public void setReason(String value) {
                    m_reason = value;
                }

                public void Run(Node xmlNode)
                throws Exception {
                    m_excludeSection.addTest(m_name, m_reason);
                }
            }

            @IXTypeReflector(name="dir")
            public class XDir implements IXTask
            {
                @IXSetterReflector(name="path")
                public void setPath(String value) {
                    m_excludeSection.openDir(value);
                }

                private String m_reason = "";
                @IXSetterReflector(name="reason")
                public void setReason(String value) {
                    m_excludeSection.setDirReason(value);
                }

                public void Run(Node xmlNode)
                throws Exception {
                    m_excludeSection.closeDir();
                }
            }
        }
    }
}
