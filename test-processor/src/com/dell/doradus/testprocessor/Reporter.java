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
    static TestSuiteInfo m_suiteInfo;

    static int m_tableWidthPx = 800;
    static int m_directoryLengthPx;
    static int m_testNameLengthPx;

    static public String generateHtmlReport(TestSuiteInfo suiteInfo)
    {
        m_suiteInfo = suiteInfo;

        setMaxDirectoryAndNameLengths(m_suiteInfo);

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
        htmlReport.append("  .column-test-1        { width:" + m_testNameLengthPx + "px; }\r\n");
        htmlReport.append("  .column-test-2        { width:" + (m_tableWidthPx - m_testNameLengthPx) + "px; }\r\n");
        htmlReport.append("  .row-test-excluded    { color:#777777; font-weight:normal; font-style:italic; }\r\n");
        htmlReport.append("  .row-test-not-started { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-aborted     { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-succeeded   { color:#008822; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-failed      { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-result-created   { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
        htmlReport.append("  .row-test-unknown     { color:#CC0000; font-weight:normal; font-style:normal; }\r\n");
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

        int cntTestsAborted = 0;
        int cntTestsFailed  = 0;

        for (TestDirInfo dirInfo : suiteInfo.getTestDirInfoList())
        {
            if (dirInfo.isExcluded())
            {
                htmlReport.append("<table class=\"table-dir table-dir-excluded\">\r\n");
                htmlReport.append("<tr>\r\n");
                htmlReport.append("<td width=\"" + m_directoryLengthPx + "px\">" +
                        dirInfo.path() +
                        "</td>\r\n");
                htmlReport.append("<td width=\"" + (m_tableWidthPx - m_directoryLengthPx) + "px\">" +
                        "excluded: " + dirInfo.reasonToExclude() +
                        "</td>\r\n");
                htmlReport.append("</tr>\r\n");
                htmlReport.append("</table>\r\n");
                continue;
            }

            htmlReport.append("<table class=\"table-dir table-dir-included\">\r\n");
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td width=\"100%\">" + dirInfo.path() + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
            htmlReport.append("</table>\r\n");

            htmlReport.append("<table class=\"table-test\">\r\n");
            for (TestInfo testInfo : dirInfo.testInfoList())
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
                if (!testInfo.isStarted()) {
                    htmlReport.append("<tr class=\"row-test-not-started\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "not started" +
                            "</td>\r\n");
                    htmlReport.append("</tr>\r\n");
                    continue;
                }
                if (testInfo.isAborted()) {
                    cntTestsAborted += 1;
                    String abortHref = "aborted" + cntTestsAborted;
                    testInfo.abortHref(abortHref);

                    htmlReport.append("<tr class=\"row-test-aborted\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "<a href=\"#" + abortHref + "\">aborted</a>" +
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
                if (testInfo.isFailed()) {
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
                    continue;
                }
                if (testInfo.isResultCreated()) {
                    htmlReport.append("<tr class=\"row-result-created\">\r\n");
                    htmlReport.append("<td class=\"column-test-1\">" +
                            testInfo.name() +
                            "</td>\r\n");
                    htmlReport.append("<td class=\"column-test-2\">" +
                            "result created" +
                            "</td>\r\n");
                    htmlReport.append("</tr>\r\n");
                    continue;
                }

                htmlReport.append("<tr class=\"row-test-unknown\">\r\n");
                htmlReport.append("<td class=\"column-test-1\">" +
                        testInfo.name() +
                        "</td>\r\n");
                htmlReport.append("<td class=\"column-test-2\">" +
                        "???" +
                        "</td>\r\n");
                htmlReport.append("</tr>\r\n");
            }

            htmlReport.append("</table>\r\n");
        }

        for (TestDirInfo dirInfo : suiteInfo.getTestDirInfoList()) {
            for (TestInfo testInfo : dirInfo.testInfoList()) {
                if (testInfo.diffHref() != null)
                {
                    String diffFilePath = dirInfo.path() + File.separator + testInfo.name() + Data.DIFF_RESULT_EXTENSION;
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
                if (testInfo.abortHref() != null)
                {
                    String testFilePath = dirInfo.path() + File.separator + testInfo.name() + Data.TEST_SCRIPT_EXTENSION;
                    String reason = testInfo.reasonToAbort();
                    if (reason == null) reason = "Aborted by unknown reason";

                    htmlReport.append("<hr align=\"left\" width=\"60%\" size=\"1\" color=\"#000000\"/>");
                    htmlReport.append("<a name=\"" + testInfo.abortHref() + "\"></a>\r\n");
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
        htmlReport.append("  .summary-column-1 { width: 20%; text-indent:20px; }\r\n");
        htmlReport.append("  .summary-column-2 { width: 80%; }\r\n");
    }

    static private void addSummary(StringBuilder htmlReport)
    {
        TestsSummary summary = new TestsSummary(m_suiteInfo);

        htmlReport.append("<table class=\"summary-table\">\r\n");

        htmlReport.append("<tr>\r\n");
        htmlReport.append("<td class=\"summary-column-1\">Tests included:</td>\r\n");
        htmlReport.append("<td class=\"summary-column-2\">" + summary.nTestsIncluded() + "</td>\r\n");
        htmlReport.append("</tr>\r\n");

        if (summary.nTestsStarted() < summary.nTestsIncluded()) {
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td class=\"summary-column-1\">Tests started:</td>\r\n");
            htmlReport.append("<td class=\"summary-column-2\">" + summary.nTestsStarted() + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
        }

        htmlReport.append("<tr>\r\n");
        htmlReport.append("<td class=\"summary-column-1\">Tests succeeded:</td>\r\n");
        htmlReport.append("<td class=\"summary-column-2\">" + summary.nTestsSucceeded() + "</td>\r\n");
        htmlReport.append("</tr>\r\n");

        if (summary.nTestsFailed() > 0) {
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td class=\"summary-column-1\">Tests failed:</td>\r\n");
            htmlReport.append("<td class=\"summary-column-2\">" + summary.nTestsFailed() + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
        }

        if (summary.nTestsAborted() > 0) {
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td class=\"summary-column-1\">Tests aborted:</td>\r\n");
            htmlReport.append("<td class=\"summary-column-2\">" + summary.nTestsAborted() + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
        }

        if (summary.nResultsCreated() > 0) {
            htmlReport.append("<tr>\r\n");
            htmlReport.append("<td class=\"summary-column-1\">Results created:</td>\r\n");
            htmlReport.append("<td class=\"summary-column-2\">" + summary.nResultsCreated() + "</td>\r\n");
            htmlReport.append("</tr>\r\n");
        }

        htmlReport.append("</table>\r\n");
        htmlReport.append("<hr align=\"left\" width=\"60%\" size=\"2\" color=\"#000000\"/>");
    }

    static public String generateXmlSummaryForCCNet(TestSuiteInfo suiteInfo)
    {
        StringBuilder xmlSummary = new StringBuilder();
        xmlSummary.append("<tests-summary>\r\n");

        TestsSummary summary = new TestsSummary(suiteInfo);
        xmlSummary.append("<included>"        + summary.nTestsIncluded()  + "</included>\r\n");
        xmlSummary.append("<started>"         + summary.nTestsStarted()   + "</started>\r\n");
        xmlSummary.append("<succeeded>"       + summary.nTestsSucceeded() + "</succeeded>\r\n");
        xmlSummary.append("<failed>"          + summary.nTestsFailed()    + "</failed>\r\n");
        xmlSummary.append("<aborted>"         + summary.nTestsAborted()   + "</aborted>\r\n");
        xmlSummary.append("<results-created>" + summary.nResultsCreated() + "</results-created>\r\n");

        xmlSummary.append("</tests-summary>\r\n");
        return xmlSummary.toString();
    }
}
