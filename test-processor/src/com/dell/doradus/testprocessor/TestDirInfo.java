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

import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

import java.util.ArrayList;
import java.util.List;

public class TestDirInfo
{
    private TestSuiteInfo   m_testSuiteInfo;
    private String          m_path;
    private List<TestInfo>  m_testInfoList;
    private boolean         m_isExcluded;
    private String          m_reasonToExclude;

    public TestDirInfo(TestSuiteInfo testSuiteInfo, String path) {
        m_testSuiteInfo   = testSuiteInfo;
        m_path            = path;
        m_testInfoList    = new ArrayList<TestInfo>();
        m_isExcluded      = false;
        m_reasonToExclude = "";
    }

    public TestSuiteInfo testSuiteInfo()
        { return m_testSuiteInfo; }

    public void path(String value)
        { m_path = value;}
    public String path()
        { return m_path;}

    public List<TestInfo> testInfoList()
        { return m_testInfoList; }

    public void isExcluded(boolean value)
        { m_isExcluded = value; }
    public boolean isExcluded()
        { return m_isExcluded; }

    public void reasonToExclude(String value)
        { m_reasonToExclude = value;}
    public String reasonToExclude()
        { return m_reasonToExclude;}

    public void includeTest(String testName)
    {
        TestInfo testInfo = findTestInfo(testName);
        if (testInfo == null) {
            testInfo = new TestInfo(this, testName);
            m_testInfoList.add(testInfo);
        }

        testInfo.isExcluded(false);
    }

    public void excludeTest(String testName, String reason)
    {
        TestInfo testInfo = findTestInfo(testName);
        if (testInfo == null) return;

        testInfo.isExcluded(true);
        testInfo.reasonToExclude(reason);
    }

    private TestInfo findTestInfo(String testName)
    {
        for (TestInfo info : m_testInfoList) {
            if (info.name().equalsIgnoreCase(testName))
                return info;
        }
        return null;
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";
        StringBuilder result = new StringBuilder();

        result.append(prefix + "Directory: " + m_path + Utils.EOL);
        result.append(prefix + "  | isExcluded =  " + m_isExcluded +
                " [reason: " + StringUtils.nullOrString(m_reasonToExclude) + "]" + Utils.EOL);

        for (TestInfo testInfo : m_testInfoList) {
            result.append(testInfo.toString(prefix + "  | ") + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
