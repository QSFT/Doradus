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

public class TestsSummary
{
    private int m_nTestsIncluded;
    private int m_nTestsStarted;
    private int m_nTestsAborted;
    private int m_nResultsCreated;
    private int m_nTestsSucceeded;
    private int m_nTestsFailed;

    public int nTestsIncluded()  { return m_nTestsIncluded;  }
    public int nTestsStarted()   { return m_nTestsStarted;   }
    public int nTestsAborted()   { return m_nTestsAborted;   }
    public int nResultsCreated() { return m_nResultsCreated; }
    public int nTestsSucceeded() { return m_nTestsSucceeded; }
    public int nTestsFailed()    { return m_nTestsFailed;    }

    public TestsSummary(TestSuiteInfo suiteInfo)
    {
        m_nTestsIncluded  = 0;
        m_nTestsStarted   = 0;
        m_nTestsAborted   = 0;
        m_nResultsCreated = 0;
        m_nTestsSucceeded = 0;
        m_nTestsFailed    = 0;

        for (TestDirInfo dirInfo : suiteInfo.getTestDirInfoList()) {
            if (dirInfo.isExcluded()) continue;

            for (TestInfo testInfo : dirInfo.testInfoList()) {
                if (testInfo.isExcluded()) continue;

                m_nTestsIncluded += 1;
                if (testInfo.isStarted())
                    m_nTestsStarted += 1;
                if (testInfo.isAborted())
                    m_nTestsAborted += 1;
                if (testInfo.isSucceeded())
                    m_nTestsSucceeded += 1;
                if (testInfo.isFailed())
                    m_nTestsFailed += 1;
                if (testInfo.isResultCreated())
                    m_nResultsCreated += 1;
            }
        }
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";
        StringBuilder result = new StringBuilder();

        result.append(prefix + "Tests included:  " + m_nTestsIncluded  + Utils.EOL);
        result.append(prefix + "Tests started:   " + m_nTestsStarted  + Utils.EOL);
        result.append(prefix + "Tests succeeded: " + m_nTestsSucceeded + Utils.EOL);

        if (m_nTestsFailed > 0)
            result.append(prefix + "Tests failed:    " + m_nTestsFailed + Utils.EOL);
        if (m_nTestsAborted > 0)
            result.append(prefix + "Tests aborted:   " + m_nTestsAborted + Utils.EOL);
        if (m_nResultsCreated > 0)
            result.append(prefix + "Results created: " + m_nResultsCreated + Utils.EOL);

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
