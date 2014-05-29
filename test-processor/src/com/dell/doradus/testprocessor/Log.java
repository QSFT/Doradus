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

import com.dell.doradus.testprocessor.common.*;

import java.io.PrintWriter;
import java.util.Date;

public class Log
{
    static private String      m_logFilePath = null;
    static private PrintWriter m_printWriter = null;

    static public void toFile(String path)
    throws Exception
    {
        try
        {
            m_logFilePath = (path != null)
                ? StringUtils.trim(path, "\r\n \t")
                : null;

            close();

            if (m_logFilePath != null && m_logFilePath.length() > 0) {
                m_printWriter = new PrintWriter(m_logFilePath);
                m_printWriter.println("----------------------------");
                m_printWriter.println(Utils.currentGMTDateTime());
                m_printWriter.println("----------------------------");
            }
        } catch(Exception ex) {
            String msg = "Failed to create Log File \"" + path + "\": " + ex.getMessage();
            throw new Exception(msg);
        }
    }

    static public boolean isOpened() {
        return m_printWriter != null;
    }

    static public void print(String text) {
        if (m_printWriter == null) return;
        if (text == null) text = "<null>";
        m_printWriter.print(text);
    }

    static public void println() {
        if (m_printWriter == null) return;
        m_printWriter.println();
    }

    static public void println(String text) {
        if (m_printWriter == null) return;
        if (text == null) text = "<null>";
        m_printWriter.println(text);
    }

    static public void close()
    {
        if (m_printWriter != null) {
            m_printWriter.close();
            m_printWriter = null;
        }
    }
}
