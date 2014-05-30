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

public class TestInfo
{
    private TestDirInfo m_testDirInfo;
    private String      m_name;
    private boolean     m_excluded;
    private String      m_reasonToExclude;
    private boolean     m_interrupted;
    private String      m_reasonToInterrupt;
    private boolean     m_executed;
    private boolean     m_requiredResultFileCreated;
    private boolean     m_succeeded;
    private String      m_diffHref;
    private String      m_interruptionHref;

    public TestInfo(TestDirInfo testDirInfo, String name) {
        m_testDirInfo               = testDirInfo;
        m_name                      = name;
        m_excluded                  = false;
        m_reasonToExclude           = "";
        m_interrupted               = false;
        m_reasonToInterrupt         = null;
        m_executed                  = false;
        m_requiredResultFileCreated = false;
        m_succeeded                 = false;
        m_diffHref                  = null;
        m_interruptionHref          = null;
    }

    // Test directory to which this test belongs
    public TestDirInfo testDirInfo()
    { return m_testDirInfo; }

    // Test name
    public void name(String value)
    { m_name = value; }
    public String name()
    { return m_name;  }

    // Is this test is excluded
    public void isExcluded(boolean value)
    { m_excluded = value; }
    public boolean isExcluded()
    { return m_excluded; }

    // Reason to exclude this test
    public void reasonToExclude(String value)
    { m_reasonToExclude = value;}
    public String reasonToExclude()
    { return m_reasonToExclude;}

    // Is execution of this test interrupted
    public void isInterrupted(boolean value)
    { m_interrupted = value; }
    public boolean isInterrupted()
    { return m_interrupted; }

    // Reason to interrupt this test
    public void reasonToInterrupt(String value)
    { m_reasonToInterrupt = value;}
    public String reasonToInterrupt()
    { return m_reasonToInterrupt;}

    // Is this test executed (without interruption)
    public void isExecuted(boolean value)
    { m_executed = value; }
    public boolean isExecuted()
    { return m_executed; }

    // Is new required result file created
    // (in case it was absent before)
    public void requiredResultFileCreated(boolean value)
    { m_requiredResultFileCreated = value; }
    public boolean requiredResultFileCreated()
    { return m_requiredResultFileCreated; }

    // Is this test succeeded (meaning that obtained
    // result is the same as required result)
    public void isSucceeded(boolean value)
    { m_succeeded = value; }
    public boolean isSucceeded()
    { return m_succeeded; }

    // Used by the Reporter
    public void diffHref(String value)
    { m_diffHref = value; }
    public String diffHref()
    { return m_diffHref; }

    // Used by the Reporter
    public void interruptionHref(String value)
    { m_interruptionHref = value; }
    public String interruptionHref()
    { return m_interruptionHref; }

    public String resultToString()
    {
        if (isExcluded())
            return "excluded";
        if (isInterrupted())
            return "interrupted";
        if (!isExecuted())
            return "not executed";
        if (isSucceeded())
            return "succeeded";
        return "failed";
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        result.append(prefix + "Test: " + m_name);
        if (m_excluded) {
            result.append(" excluded");
            if (m_reasonToExclude != null && m_reasonToExclude.trim().length() > 0) {
                result.append(" [reason: " + m_reasonToExclude + "]");
            }
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
