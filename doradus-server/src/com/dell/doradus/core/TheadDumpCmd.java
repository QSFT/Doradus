package com.dell.doradus.core;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.RESTCallback;

/**
 * Reply to a REST command such as: GET /_dump. Return a stack trace of all current
 * as a plain text message.
 */
public class TheadDumpCmd extends RESTCallback {

    @Override
    protected RESTResponse invoke() {
        StringBuilder dump = new StringBuilder();
        dump.append("Doradus Thread Dump @ ");
        dump.append(Utils.formatDate(System.currentTimeMillis()));
        dump.append("\n\n");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadBean.dumpAllThreads(true, true);
        for (ThreadInfo thread : threadInfos) {
            dump.append(thread.toString());
        }
        return new RESTResponse(HttpCode.OK, dump.toString());
    }

}   // TheadDumpCmd
