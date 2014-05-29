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

import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

import java.util.ArrayList;
import java.util.List;

public class DirExcluded
{
    public String m_path;
    public String m_reason;

    public List<String> m_testNames;
    public List<String> m_testReasons;

    public DirExcluded(String path, String reason) {
        m_path        = path;
        m_reason      = reason;
        m_testNames   = new ArrayList<String>();
        m_testReasons = new ArrayList<String>();
    }
    public DirExcluded(String path) {
        this(path, "");
    }
    public DirExcluded() {
        this("");
    }

    public void setPath(String value) {
        m_path = value;
    }
    public String getPath() {
        return m_path;
    }
    public void setReason(String value) {
        m_reason = value;
    }
    public String getReason() {
        return m_reason;
    }
    public List<String> getTestNames() {
        return m_testNames;
    }
    public List<String> getTestReasons() {
        return m_testReasons;
    }

    public void addTest(String name)
    {
        if (m_testNames.contains(name))
            return;

        m_testNames.add(name);
        m_testReasons.add(m_reason);
    }

    public void setTestReason(String name, String reason)
    {
        int i = m_testNames.indexOf(name);
        if (i < 0) return;

        m_testReasons.set(i, reason);
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        result.append(prefix + "Directory = " + m_path + " reason: " + m_reason + Utils.EOL);

        if (m_testNames.isEmpty())
            result.append(prefix + "|  <all tests>");
        else for (int i = 0; i < m_testNames.size(); i++) {
            result.append(prefix + "| " + m_testNames.get(i) + " reason: " + m_testReasons.get(i) + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
