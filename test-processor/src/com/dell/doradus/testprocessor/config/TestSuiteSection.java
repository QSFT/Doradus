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

import com.dell.doradus.testprocessor.TestSuiteInfo;
import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

public class TestSuiteSection
{
    private String          m_rootPath;
    private IncludeSection  m_includeSection;
    private ExcludeSection  m_excludeSection;

    public TestSuiteSection() {
        m_rootPath = "";
    }

    public void start()
    {
    }

    public void finish()
    {
        if (m_includeSection == null) {
            m_includeSection = new IncludeSection();
            m_includeSection.start(m_rootPath);
            m_includeSection.finish();
        }
        if (m_excludeSection == null) {
            m_excludeSection = new ExcludeSection();
            m_excludeSection.start(m_rootPath);
            m_excludeSection.finish();
        }
    }

    public void setRootPath(String value) {
        m_rootPath = value;
    }
    public String getRootPath() {
        return m_rootPath;
    }

    public void setIncludeSection(IncludeSection value) {
        m_includeSection = value;
    }
    public IncludeSection getIncludeSection() {
        return m_includeSection;
    }
    public void setExcludeSection(ExcludeSection value) {
        m_excludeSection = value;
    }
    public ExcludeSection getExcludeSection() {
        return m_excludeSection;
    }

    public void applyTo(TestSuiteInfo testSuiteInfo)
    throws Exception
    {
        m_includeSection.applyTo(testSuiteInfo);
        m_excludeSection.applyTo(testSuiteInfo);
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        result.append(prefix + "RootPath = " + m_rootPath + Utils.EOL);
        result.append(prefix + "### INCLUDE" + Utils.EOL);
        result.append(m_includeSection.toString(prefix + "    ") + Utils.EOL);
        result.append(prefix + "### EXCLUDE" + Utils.EOL);
        result.append(m_excludeSection.toString(prefix + "    ") + Utils.EOL);

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
