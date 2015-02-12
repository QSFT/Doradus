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
import com.dell.doradus.testprocessor.common.FileUtils;
import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class IncludeSection
{
    private List<DirIncluded>  m_dirList;
    private Stack<DirIncluded> m_dirStack;

    public void start(String rootPath)
    {
        m_dirList  = new ArrayList<DirIncluded>();
        m_dirStack = new Stack<DirIncluded>();

        DirIncluded rootDir = new DirIncluded(rootPath);
        m_dirStack.push(rootDir);
    }

    public void finish()
    {
        DirIncluded rootDir = m_dirStack.pop();

        if (!rootDir.testNames().isEmpty() || !rootDir.subDirsIncluded())
            m_dirList.add(rootDir);
    }

    public void openDir(String relativePath)
    {
        DirIncluded parentDir = m_dirStack.peek();
        String path = FileUtils.combinePaths(parentDir.path(), relativePath);
        parentDir.subDirsIncluded(true);
        m_dirStack.push(new DirIncluded(path));
    }

    public void closeDir()
    {
        m_dirList.add(m_dirStack.pop());
    }

    public void addTest(String name)
    {
        m_dirStack.peek().addTest(name);
    }

    public void applyTo(TestSuiteInfo suiteInfo)
    throws Exception
    {
        for (DirIncluded dir : m_dirList)
        {
            if (!dir.testNames().isEmpty()) {
                suiteInfo.includeTests(dir.path(), dir.testNames());
            } else if (!dir.subDirsIncluded()) {
                suiteInfo.includeDirectory(dir.path());
            }
        }
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        for (DirIncluded dir : m_dirList)
            result.append(dir.toString(prefix) + Utils.EOL);

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
