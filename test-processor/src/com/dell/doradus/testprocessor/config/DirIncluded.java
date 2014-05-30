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

public class DirIncluded
{
    public String       m_path;
    public List<String> m_testNames;

    public DirIncluded(String path) {
        m_path = path;
        m_testNames = new ArrayList<String>();
    }
    public DirIncluded() {
        this("");
    }

    public void setPath(String value) {
        m_path = value;
    }
    public String getPath() {
        return m_path;
    }
    public List<String> getTestNames() {
        return m_testNames;
    }

    public void addTest(String name)
    {
        if (m_testNames.contains(name))
            return;

        m_testNames.add(name);
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        result.append(prefix + "Directory = " + m_path + Utils.EOL);

        if (m_testNames.isEmpty())
            result.append(prefix + "|  <all tests>");
        else for (String name : m_testNames) {
            result.append(prefix + "| " + name + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
