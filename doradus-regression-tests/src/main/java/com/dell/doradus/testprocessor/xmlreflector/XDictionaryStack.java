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

package com.dell.doradus.testprocessor.xmlreflector;

public class XDictionaryStack
{
    private IXReflectedUnit[]  m_dict;

    private int     m_size;
    private int     m_dictTop;
    private int[]   m_frames;
    private int     m_framesTop;
    private boolean m_ignoreCase;

    public XDictionaryStack()
    {
        m_size       = 4096;
        m_dict       = new IXReflectedUnit[m_size];
        m_dictTop    = -1;
        m_frames     = new int[m_size];
        m_framesTop  = -1;
        m_ignoreCase = false;
    }

    public void    ignoreCase(boolean value) { m_ignoreCase = value; }
    public boolean ignoreCase()              { return m_ignoreCase;  }

    public void push(IXReflectedUnit unit)
    throws Exception
    {
        if (m_ignoreCase)
            unit.setName(unit.getName().toLowerCase());

        if (++m_dictTop >= m_size) {
            String msg = "Dictionary stack overflow: size = " + m_size;
            throw new Exception(msg);
        }

        m_dict[m_dictTop] = unit;
    }

    public IXReflectedUnit find(String name)
    {
        String key = name;
        if (m_ignoreCase)
            key = key.toLowerCase();

        for (int i = m_dictTop; i > -1; i--) {
            if (m_dict[i].getName().equals(key))
                return m_dict[i];
        }

        return null;
    }

    public void openFrame()
    {
        m_frames[++m_framesTop] = m_dictTop;
    }

    public void closeFrame()
            throws Exception
    {
        if (m_framesTop < 0) {
            String msg = "Dictionary frames stack underflow: top = " + m_framesTop;
            throw new Exception(msg);
        }

        m_dictTop = m_frames[m_framesTop--];
    }
}
