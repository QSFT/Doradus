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

package com.dell.doradus.service.db.thrift;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.AuthenticationRequest;

import com.dell.doradus.service.db.IDBAuthenticator;

public class SimpleAuthenticator implements IDBAuthenticator{
	
    public final static String PASSWD_FILENAME_PROPERTY        = "passwd.properties";
    public static final String USERNAME_KEY                    = "dbusername";
    public static final String PASSWORD_KEY                    = "dbpassword";
    private static final String DEFAULT_CONFIGFILE             = "config/doradus.properties";
    
	@Override
	public AuthenticationRequest getAuthRequest()
	{
		String pfilename = System.getProperty(PASSWD_FILENAME_PROPERTY);
		if(pfilename == null)
			pfilename = DEFAULT_CONFIGFILE;
		String userName = null;
		String password = null;
        InputStream in = null;
        try
        {
            in = new BufferedInputStream(new FileInputStream(pfilename));
            Properties props = new Properties();
            props.load(in);
            userName = props.getProperty(USERNAME_KEY);
            password = props.getProperty(PASSWORD_KEY);
        } catch (IOException e) {
			e.printStackTrace();
		}
        finally {
            if(in != null){
                try {
                    in.close();
                } 
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }		
		AuthenticationRequest authRequest = new AuthenticationRequest();
		Map<String, String> credentials = new HashMap<String, String>();
		credentials.put(IAuthenticator.USERNAME_KEY, userName);
		credentials.put(IAuthenticator.PASSWORD_KEY, password);
		authRequest.setCredentials(credentials);
        return authRequest;
	}

}
