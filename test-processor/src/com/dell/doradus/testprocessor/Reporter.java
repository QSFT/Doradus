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
import com.dell.doradus.testprocessor.common.XmlUtils;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Reporter
{
    static TestSuiteInfo m_testSuiteInfo;

    static int m_tableWidthPx = 800;
    static int m_directoryLengthPx;
    static int m_testNameLengthPx;

    static public String generateXmlSummaryForCCNet(TestSuiteInfo testSuiteInfo)
    {
        int cntSucceeded    = 0;
        int cntFailed       = 0;
        int cntInterrupted  = 0;
        int cntNotExecuted  = 0;

        for (TestDirInfo testDirInfo : testSuiteInfo.getTestDirInfoList()) {
            if (testDirInfo.isExcluded()) continue;

            for (TestInfo testInfo : testDirInfo.testInfoList()) {
                if (testInfo.isExcluded())
                    continue;

                if (testInfo.isInterrupted())
                    { cntInterrupted += 1; continue; }
                if (testInfo.isSucceeded())
                    { cntSucceeded += 1; continue; }
                if (!testInfo.isExecuted())
                    { cntNotExecuted += 1; continue; }
                cntFailed += 1;
            }
        }

        StringBuilder xmlSummary = new StringBuilder();
        xmlSummary.append("<tests-summary>\r\n");

        xmlSummary.append("<succeeded>"    + cntSucceeded   + "</succeeded>\r\n");
        xmlSummary.append("<failed>"       + cntFailed      + "</failed>\r\n");
        xmlSummary.append("<interrupted>"  + cntInterrupted + "</interrupted>\r\n");
        xmlSummary.append("<not-executed>" + cntNotExecuted + "</not-executed>\r\n");

        xmlSummary.append("</tests-summary>\r\n");
        return xmlSummary.toString();
    }

    static public String generateHtmlReport(TestSuiteInfo testSuiteInfo)
    {
        m_testSuiteInfo = testSuiteInfo;

        setMaxDirectoryAndNameLengths(testSuiteInfo);

        StringBuilder htmlReport = new StringBuilder();

        htmlReport.append("<html>\r\n");
        htmlReport.append("<head>\r\n");
        htmlReport.append("<style type=\"text/css\">\r\n");

        addReportHeadStyle(htmlReport);
        addSummaryStyle(htmlReport);

        htmlReport.append("  .table-summary  { border-collapse: collapse; width:800px; background-color:#FFFFFF;\r\n");
        htmlReport.append("                    font-family: verdana, helvetica, arial;\r\n");
        htmlReport.append("                    font-size: 11px; font-weight:normal;; font-style:normal; }\r\n");
        htmlReport.append("  .column-summary-1 { width: 15%; text-indent:20px; }\r\n");
        htmlReport.append("  .column-summary-2 { width: 85%; }\r\n");
        htmlReport.append("  .table-dir   { border-collapse: collapse; width:800px; background-color:#FFFFFF;\r\n");
        htmlReport.append("                 font-family: verdana, helvetica, arial; font-size: 11px;}\r\n");
        htmlReport.append("  .table-dir-included { color:#000000; font-weight:normal;; font-style:normal; }\r\n");
        htmlReport.append("  .table-dir-excluded { color:#777777; font-weight:normal;; font-style:italic; }\r\n");
        htmlReport.append("  .table-test { border-collapse: collapse; width:800px; background-color:#FFFFFF;\r\n");
        htmlReport.append("                text-indent:20px;\r\n");
        htmlReport.append("                font-family: verdana, helvetica, arial; font-size: 11px; }\r\n");
        htmlReport.append("  .column-test-1         { width:" + m_testNameLengthPx + "px; }\r\n");
        htmlReport.append("  .column-test-2         { width:" + (m_tableWidthPx - m_testNameLengthPx) + "px; }\r\n");
        htmlReport.append("  .row-test-excluded     { color:#777777; font-weight:normal; font-style:italic; }\r\n");
        htmlReport.append("  .row-test-succeeded    { color:#008822; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-failed       { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-interrupted  { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-not-executed { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .attach      { background-color:#FFFFFF; color:#000000; }\r\n");
        htmlReport.append("  .attach-head { font-family: verdana, helvetica, arial;\r\n");
        htmlReport.append("                 font-size: 10px; font-weight:bold; font-style:normal; }\r\n");
        htmlReport.append("  .attach-body { font-family: consolas, courier new;\r\n");
        htmlReport.append("                 font-size: 11px; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("</style>\r\n");
        htmlReport.append("</head>\r\n");
        htmlReport.append("<body link=\"#CC0000\" vlink=\"#CC0000\" alink=\"#CC0000\">\r\n");

        addReportHead(htmlReport);
        addSummary(htmlReport);

        int cntTestsInterrupted = 0;
        int cntTestsFailed      = 0;

        for (TestDirInfo testDirInfo : testSuiteInfo.getTestDirInfoList())
        {
            if (testDirInfo.isExcluded())
            {
                htmlReport.append("<table class=\"table-dir table-dir-excluded\">\r\n");
                htmlReport.append("<tr>\r\n");
                htmlReport.append("<td width=\"" + m_directoryLengthPx + "px\">" +
                        testDirInfo.path() +
                        "</td>\r\n");
                htmlReport.append("<td width=\"" + (m_tableWidthPx - m_directoryLengthPx) + "px\">" +
                        "excluded: " + testDirInfo.reasonToExclude() +
                        "</td>\r\n");
                htmlReport.append("</tr>\r\n");
                htmlReport.append("</table>\r\n");
                continue;
            }

            htmlReport.append("<table class=\"table-dir table-dir-included\">\r\n");
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td width=\"100%\">" +
                    testDirInfo.path() +
                    "</td>\r\n");
            htmlReport.append("</tr>\r\n");
            htmlReport.append("</table>\r\n");

            htmlReport.append("<table class=\"table-test\">\r\n");
            for (TestInfo testInfo : testDirInfo.testInfoList())
            {
                if (testInfo.isExcluded()) {
                    htmlReport.append("<tr class=\"row-test-excluded\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "excluded: " + testInfo.reasonToExclude() +
                            "</td>\r\n");
                    htmlReport.append("</tr>\r\n");
                    continue;
                }
                if (testInfo.isInterrupted()) {
                    cntTestsInterrupted += 1;
                    String interruptionHref = "interrupted" + cntTestsInterrupted;
                    testInfo.interruptionHref(interruptionHref);

                    htmlReport.append("<tr class=\"row-test-interrupted\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "<a href=\"#" + interruptionHref + "\">interrupted</a>" +
                            "</td>\r\n");
                    htmlReport.append("</tr>\r\n");
                    continue;
                }
                if (!testInfo.isExecuted()) {
                    htmlReport.append("<tr class=\"row-test-not-executed\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "not executed" +
                            "</td>\r\n");
                    htmlReport.append("</tr>\r\n");
                    continue;
                }
                if (testInfo.isSucceeded()) {
                    htmlReport.append("<tr class=\"row-test-succeeded\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "succeeded" +
                            "</td>\r\n");
                    htmlReport.append("</tr>\r\n");
                    continue;
                }

                cntTestsFailed += 1;
                String diffHref = "diff" + cntTestsFailed;
                testInfo.diffHref(diffHref);

                htmlReport.append("<tr class=\"row-test-failed\">\r\n");
                htmlReport.append("<td class=\"column-test-1\">" +
                        testInfo.name() +
                        "</td>\r\n");
                htmlReport.append("<td class=\"column-test-2\">" +
                        "<a href=\"#" + diffHref + "\">failed</a>" +
                        "</td>\r\n");
                htmlReport.append("</tr>\r\n");
            }

            htmlReport.append("</table>\r\n");
        }

        for (TestDirInfo testDirInfo : testSuiteInfo.getTestDirInfoList()) {
            for (TestInfo testInfo : testDirInfo.testInfoList()) {
                if (testInfo.diffHref() != null)
                {
                    String diffFilePath = testDirInfo.path() + "\\" + testInfo.name() + Data.DIFF_RESULT_EXTENSION;
                    String diffContent;
                    if (FileUtils.fileExists(diffFilePath)) {
                        try { diffContent = FileUtils.readAllText(diffFilePath); }
                        catch(Exception ex) {
                            diffContent = "Failed to read diff file \"" + diffFilePath + "\": " + ex.getMessage();
                        }
                    } else {
                        diffContent = "Diff file \"" + diffFilePath + "\" not found";
                    }

                    htmlReport.append("<hr align=\"left\" width=\"60%\" size=\"1\" color=\"#000000\"/>");
                    htmlReport.append("<a name=\"" + testInfo.diffHref() + "\"></a>\r\n");
                    htmlReport.append("<p class=\"attach attach-head\">" + diffFilePath + "</p>\r\n");

                    htmlReport.append("<PRE class=\"attach attach-body\">\r\n");
                    htmlReport.append(XmlUtils.escapeXml(diffContent));
                    htmlReport.append("</PRE>\r\n");
                }
                if (testInfo.interruptionHref() != null)
                {
                    String testFilePath = testDirInfo.path() + "\\" + testInfo.name() + Data.TEST_SCRIPT_EXTENSION;
                    String reason = testInfo.reasonToInterrupt();
                    if (reason == null) reason = "Interrupted by unknown reason";

                    htmlReport.append("<hr align=\"left\" width=\"60%\" size=\"1\" color=\"#000000\"/>");
                    htmlReport.append("<a name=\"" + testInfo.interruptionHref() + "\"></a>\r\n");
                    htmlReport.append("<p class=\"attach attach-head\">" + testFilePath + "</p>\r\n");

                    htmlReport.append("<PRE class=\"attach attach-body\">\r\n");
                    htmlReport.append(XmlUtils.escapeXml(reason));
                    htmlReport.append("</PRE>\r\n");
                }
            }
        }

        htmlReport.append("</body>\r\n");
        htmlReport.append("</html>\r\n");

        return htmlReport.toString();
    }

    static private void setMaxDirectoryAndNameLengths(TestSuiteInfo testSuiteInfo)
    {
        int maxDirectoryLength = 0;
        int maxTestNameLength  = 0;

        for (TestDirInfo testDirInfo : testSuiteInfo.getTestDirInfoList())
        {
            int directoryLength = testDirInfo.path().length();
            if (maxDirectoryLength < directoryLength)
                maxDirectoryLength = directoryLength;

            for (TestInfo testInfo : testDirInfo.testInfoList())
            {
                int nameLength = testInfo.name().length();
                if (maxTestNameLength < nameLength)
                    maxTestNameLength = nameLength;
            }
        }

        m_directoryLengthPx = maxDirectoryLength * 7 + 20;
        m_testNameLengthPx  = maxTestNameLength  * 7 + 20;
    }

    static private void addReportHeadStyle(StringBuilder htmlReport)
    {
        htmlReport.append("  .report-head { font-family: verdana, helvetica, arial;\r\n");
        htmlReport.append("                 font-size: 11px; font-weight:bold; font-style:normal; }\r\n");
    }
    static private void addReportHead(StringBuilder htmlReport)
    {
        htmlReport.append("<p class=\"report-head\">Test Results</p>\r\n");
        htmlReport.append("<hr align=\"left\" width=\"60%\" size=\"2\" color=\"#000000\"/>\r\n");
    }

    static private void addSummaryStyle(StringBuilder htmlReport)
    {
        htmlReport.append("  .summary-table    { border-collapse: collapse; width:800px; background-color:#FFFFFF;\r\n");
        htmlReport.append("                      font-family: verdana, helvetica, arial;\r\n");
        htmlReport.append("                      font-size: 11px; font-weight:normal;; font-style:normal; }\r\n");
        htmlReport.append("  .summary-column-1 { width: 15%; text-indent:20px; }\r\n");
        htmlReport.append("  .summary-column-2 { width: 85%; }\r\n");
    }
    static private void addSummary(StringBuilder htmlReport)
    {
        int cntExcluded    = 0;
        int cntNotExecuted = 0;
        int cntInterrupted = 0;
        int cntSucceeded   = 0;
        int cntFailed      = 0;

        for (TestDirInfo testDirInfo : m_testSuiteInfo.getTestDirInfoList()) {
            for (TestInfo testInfo : testDirInfo.testInfoList()) {
                if (testInfo.isExcluded())
                    cntExcluded += 1;
                else if (testInfo.isInterrupted())
                    cntInterrupted += 1;
                else if (!testInfo.isExecuted())
                    cntNotExecuted += 1;
                else if (testInfo.isSucceeded())
                    cntSucceeded += 1;
                else
                    cntFailed += 1;
            }
        }

        htmlReport.append("<table class=\"summary-table\">\r\n");
        htmlReport.append("<tr>\r\n");
        htmlReport.append("<td class=\"summary-column-1\">Succeeded:</td>\r\n");
        htmlReport.append("<td class=\"summary-column-2\">" + cntSucceeded + "</td>\r\n");
        htmlReport.append("</tr>\r\n");
        htmlReport.append("<tr>\r\n");
        htmlReport.append("<td class=\"summary-column-1\">Failed:</td>\r\n");
        htmlReport.append("<td class=\"summary-column-2\">" + cntFailed + "</td>\r\n");
        htmlReport.append("</tr>\r\n");
        if (cntInterrupted > 0) {
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td class=\"summary-column-1\">Interrupted:</td>\r\n");
            htmlReport.append("<td class=\"summary-column-2\">" + cntInterrupted + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
        }
        if (cntNotExecuted > 0) {
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td class=\"summary-column-1\">Not executed:</td>\r\n");
            htmlReport.append("<td class=\"summary-column-2\">" + cntNotExecuted + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
        }
        htmlReport.append("</table>\r\n");
        htmlReport.append("<hr align=\"left\" width=\"60%\" size=\"2\" color=\"#000000\"/>");
    }
}
