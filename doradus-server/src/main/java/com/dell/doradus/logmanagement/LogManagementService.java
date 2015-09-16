package com.dell.doradus.logmanagement;

import com.dell.doradus.service.Service;
import com.dell.doradus.service.schema.SchemaService;

public class LogManagementService extends Service {
	
    private static final LogManagementService logManagementService = new LogManagementService();
    
    /**
     * Get the singleton instance of the LogManagementService. The object may or may not have
     * been initialized yet.
     * 
     * @return  The singleton instance of the LogManagementService.
     */ 
    public static LogManagementService instance() {
        return logManagementService;
    } 
    
    private LogManagementService() {}
    
    

	@Override
	protected void initService() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void startService() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void stopService() {
		// TODO Auto-generated method stub
		
	}

}
