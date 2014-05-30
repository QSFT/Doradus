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

import com.dell.doradus.testprocessor.common.FileUtils;
import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestSuiteInfo
{
    private List<TestDirInfo> m_testDirInfoList;

    public TestSuiteInfo() {
        m_testDirInfoList = new ArrayList<TestDirInfo>();
    }

    public List<TestDirInfo> getTestDirInfoList()
        { return m_testDirInfoList; }

    public void add(TestDirInfo dirInfo)
    {
        TestDirInfo existingDirInfo = findExistingDir(dirInfo);
        if (existingDirInfo != null) {
            for (TestInfo testInfo : dirInfo.testInfoList())
                existingDirInfo.add(testInfo);
        } else {
            m_testDirInfoList.add(dirInfo);
        }
    }

    public void includeWholeDirectory(TestDirInfo dirInfoToInclude)
    throws Exception
    {
        TestDirInfo dirToRemove = findExistingDir(dirInfoToInclude);
        m_testDirInfoList.remove(dirToRemove);

        File   dir  = new File(dirInfoToInclude.path());
        File[] list = dir.listFiles();
        if (list == null) {
            String msg = "Not directory: '" + dirInfoToInclude.path() + "'";
            throw new Exception(msg);
        }

        List<TestDirInfo> subdirsToInclude = new ArrayList<TestDirInfo>();

        for (File file : list) {
            String path = file.getAbsolutePath();
            if (file.isFile() && path.endsWith(Data.TEST_SCRIPT_EXTENSION)) {
                dirInfoToInclude.includeTest(FileUtils.getTestName(path));
            } else if (file.isDirectory()) {
                TestDirInfo dirInfo = new TestDirInfo(path);
                subdirsToInclude.add(dirInfo);
            }
        }

        if (dirInfoToInclude.cntTests() > 0) {
            m_testDirInfoList.add(dirInfoToInclude);
        }

        for (TestDirInfo subdir : subdirsToInclude) {
            includeWholeDirectory(subdir);
        }
    }

    public void excludeWholeDirectory(TestDirInfo dirInfoToExclude)
    {
        List<TestDirInfo> dirsToRemove = findExistingDirAndSubdirs(dirInfoToExclude);
        for (TestDirInfo dir : dirsToRemove)
            m_testDirInfoList.remove(dir);
        m_testDirInfoList.add(dirInfoToExclude);
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        for (TestDirInfo testDirInfo : m_testDirInfoList) {
            result.append(testDirInfo.toString(prefix) + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }

    private TestDirInfo findExistingDir(TestDirInfo infoToFind)
    {
        for (TestDirInfo existingInfo : m_testDirInfoList) {
            if (existingInfo.path().equalsIgnoreCase(infoToFind.path()))
                return existingInfo;
        }
        return null;
    }

    private List<TestDirInfo> findExistingDirAndSubdirs(TestDirInfo rootInfo)
    {
        List<TestDirInfo> result = new ArrayList<TestDirInfo>();
        String root = rootInfo.path();

        for (TestDirInfo existingInfo : m_testDirInfoList) {
            Path path = Paths.get(existingInfo.path());
            if (path.startsWith(root))
                result.add(existingInfo);
        }

        return result;
    }
}
