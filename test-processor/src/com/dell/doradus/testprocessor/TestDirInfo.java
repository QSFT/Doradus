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
    private String          m_path;
    private List<TestInfo>  m_testInfoList;
    private boolean         m_excluded;
    private String          m_reason;

    public TestDirInfo(String path) {
        this(path, false, "");
    }
    public TestDirInfo(String path, boolean excluded, String reason) {
        m_path          = path;
        m_testInfoList  = new ArrayList<TestInfo>();
        m_excluded      = excluded;
        m_reason        = reason;
    }

    public void path(String value)
        { m_path = value;}
    public String path()
        { return m_path;}
    public void isExcluded(boolean value)
        { m_excluded = value; }
    public boolean isExcluded()
        { return m_excluded; }
    public void reasonToExclude(String value)
        { m_reason = value;}
    public String reasonToExclude()
        { return m_reason;}
    public List<TestInfo> testInfoList()
        { return m_testInfoList; }

    public int cntTests()
        { return m_testInfoList.size(); }

    public TestInfo includeTest(String name)
    {
        TestInfo testInfo = new TestInfo(this, name);
        testInfo.isExcluded(false);
        add(testInfo);
        return testInfo;
    }

    public TestInfo excludeTest(String name, String reason)
    {
        TestInfo testInfo = new TestInfo(this, name);
        testInfo.isExcluded(true);
        testInfo.reasonToExclude(reason);
        add(testInfo);
        return testInfo;
    }

    public void add(TestInfo testInfo) {
        remove(testInfo.name());
        m_testInfoList.add(testInfo);
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        result.append(prefix + "Directory: " + m_path + (m_excluded ? " excluded [reason: " + m_reason + "]" : "") + Utils.EOL);
        for (TestInfo testInfo : m_testInfoList) {
            result.append(testInfo.toString(prefix + "| ") + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }

    private TestInfo remove(String name) {
        TestInfo info = find(name);
        if (info != null) m_testInfoList.remove(info);
        return info;
    }

    private TestInfo find(String name)
    {
        for (TestInfo info : m_testInfoList) {
            if (info.name().equalsIgnoreCase(name))
                return info;
        }
        return null;
    }
}
