package com.dell.doradus.servlet;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import com.dell.doradus.core.DoradusServer;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.spider.SpiderService;

/**
 * Registering ServletContextListener
 */
//@WebListener
public class DoradusServletContextListener implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent sce) {  		
		DoradusServer.startEmbedded(null, SERVICES);		
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		DoradusServer.shutDown();
	}

	private static final String[] SERVICES = new String[]{
        SpiderService.class.getName(),
        OLAPService.class.getName()   
	};      
 
}
