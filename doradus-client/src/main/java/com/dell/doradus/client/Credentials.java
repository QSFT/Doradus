/*
 * DELL PROPRIETARY INFORMATION
 * 
 * This software is confidential.  Dell Inc., or one of its subsidiaries, has
 * supplied this software to you under the terms of a license agreement,
 * nondisclosure agreement or both.  You may not copy, disclose, or use this 
 * software except in accordance with those terms.
 * 
 * Copyright 2014 Dell Inc.  
 * ALL RIGHTS RESERVED.
 * 
 * DELL INC. MAKES NO REPRESENTATIONS OR WARRANTIES
 * ABOUT THE SUITABILITY OF THE SOFTWARE, EITHER EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. DELL SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE
 * AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.dell.doradus.client;

/**
 * Represents credentials for use with a Doradus application. Credentials consist of the
 * following properties:
 * <ul>
 * <li>Tenant: This is the name of a tenant ("customer"). If this name is null, the
 *     credentials request access to the default tenant.
 * <li>User id: This is the user name to use when accessing applications within the
 *     specified tenant. If this is null, requests will be performe unauthenticated.
 * <li>Password: This should be non-null when a user id is specified.
 * </ul>
 */
public class Credentials {
    private final String m_tenant;
    private final String m_userid;
    private final String m_password;
    
    public Credentials(String tenant, String userid, String password) {
        m_tenant = tenant;
        m_userid = userid;
        m_password = password;
    }
    
    public String getTenant() {
        return m_tenant;
    }
    
    public String getUserid() {
        return m_userid;
    }
    
    public String getPassword() {
        return m_password;
    }
}   // class Credentials
