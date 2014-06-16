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
    private boolean     m_isExcluded;
    private String      m_reasonToExclude;
    private boolean     m_isStarted;
    private boolean     m_isAborted;
    private String      m_reasonToAbort;
    private boolean     m_isResultCreated;
    private boolean     m_isSucceeded;
    private String      m_diffHref;
    private String      m_abortHref;

    public TestInfo(TestDirInfo testDirInfo, String name) {
        m_testDirInfo     = testDirInfo;
        m_name            = name;
        m_isExcluded      = false;
        m_reasonToExclude = null;
        m_isStarted       = false;
        m_isAborted       = false;
        m_reasonToAbort   = null;
        m_isResultCreated = false;
        m_isSucceeded     = false;
        m_diffHref        = null;
        m_abortHref       = null;
    }

    public TestDirInfo testDirInfo()
        { return m_testDirInfo; }

    public void name(String value)
        { m_name = value; }
    public String name()
        { return m_name;  }

    public void isExcluded(boolean value)
        { m_isExcluded = value; }
    public boolean isExcluded()
        { return m_isExcluded; }

    public void reasonToExclude(String value)
        { m_reasonToExclude = value;}
    public String reasonToExclude()
        { return m_reasonToExclude;}

    public void isStarted(boolean value)
        { m_isStarted = value; }
    public boolean isStarted()
        { return m_isStarted; }

    public void isAborted(boolean value)
        { m_isAborted = value; }
    public boolean isAborted()
        { return m_isAborted; }

    public void reasonToAbort(String value)
        { m_reasonToAbort = value;}
    public String reasonToAbort()
        { return m_reasonToAbort;}

    public void isResultCreated(boolean value)
        { m_isResultCreated = value; }
    public boolean isResultCreated()
        { return m_isResultCreated; }

    public void isSucceeded(boolean value)
        { m_isSucceeded = value; }
    public boolean isSucceeded()
        { return m_isSucceeded; }

    public boolean isFailed()
        { return m_isStarted &&
                !m_isAborted &&
                !m_isResultCreated &&
                !m_isSucceeded; }

    public void diffHref(String value)
        { m_diffHref = value; }
    public String diffHref()
        { return m_diffHref; }

    public void abortHref(String value)
        { m_abortHref = value; }
    public String abortHref()
        { return m_abortHref; }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";
        StringBuilder result = new StringBuilder();

        result.append(prefix + "Test: " + m_name + Utils.EOL);
        result.append(prefix + "  | isExcluded =  " + m_isExcluded +
                               " [reason: " + StringUtils.nullOrString(m_reasonToExclude) + "]" + Utils.EOL);
        result.append(prefix + "  | isStarted =   " + m_isStarted + Utils.EOL);
        result.append(prefix + "  | isAborted =   " + m_isAborted +
                " [reason: " + StringUtils.nullOrString(m_reasonToAbort) + "]" + Utils.EOL);
        result.append(prefix + "  | isSucceeded = " + m_isSucceeded + Utils.EOL);

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
