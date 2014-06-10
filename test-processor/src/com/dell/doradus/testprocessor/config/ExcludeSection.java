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

import com.dell.doradus.testprocessor.TestDirInfo;
import com.dell.doradus.testprocessor.TestInfo;
import com.dell.doradus.testprocessor.TestSuiteInfo;
import com.dell.doradus.testprocessor.common.FileUtils;
import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class ExcludeSection
{
    private List<DirExcluded>  m_dirList;
    private Stack<DirExcluded> m_dirStack;

    public void start(String rootPath)
    {
        m_dirList  = new ArrayList<DirExcluded>();
        m_dirStack = new Stack<DirExcluded>();

        DirExcluded rootDir = new DirExcluded(rootPath);
        m_dirStack.push(rootDir);
    }

    public void finish()
    {
        DirExcluded rootDir = m_dirStack.pop();

        if (!rootDir.testNames().isEmpty())
            m_dirList.add(rootDir);
    }

    public void openDir(String relativePath)
    {
        DirExcluded parentDir = m_dirStack.peek();
        String path = FileUtils.combinePaths(parentDir.path(), relativePath);
        parentDir.subDirsExcluded(true);
        m_dirStack.push(new DirExcluded(path));
    }

    public void setDirReason(String reason)
    {
        m_dirStack.peek().reason(reason);
    }

    public void closeDir()
    {
        m_dirList.add(m_dirStack.pop());
    }

    public void addTest(String name, String reason)
    {
        m_dirStack.peek().addTest(name);
        m_dirStack.peek().setTestReason(name, reason);
    }


    public void applyTo(TestSuiteInfo testSuiteInfo)
    throws Exception
    {
        for (DirExcluded dirExcluded : m_dirList)
        {
            String dirPath = dirExcluded.path();
            List<String> testNames = dirExcluded.testNames();
            if (testNames.isEmpty() && dirExcluded.subDirsExcluded())
                continue;

            TestDirInfo testDirInfo = new TestDirInfo(testSuiteInfo, dirPath, true, dirExcluded.reason());
            if (testNames.isEmpty()) {
                testSuiteInfo.excludeWholeDirectory(testDirInfo);
            } else  {
                List<String> testReasons = dirExcluded.testReasons();
                for (int i = 0; i < testNames.size(); i++) {
                    testDirInfo.excludeTest(testNames.get(i), testReasons.get(i));
                }

                testSuiteInfo.add(testDirInfo);
            }
        }
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        if (m_dirList.isEmpty())
            result.append(prefix + "<none>");
        else for (DirExcluded dir : m_dirList)
            result.append(dir.toString(prefix) + Utils.EOL);

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
