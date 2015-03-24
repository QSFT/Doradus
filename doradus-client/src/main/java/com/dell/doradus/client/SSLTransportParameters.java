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

package com.dell.doradus.client;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Holds parameters for creating a TLS/SSL connection. Methods allow the keystore and
 * truststore to be defined. {@link #createSSLContext()} can then be used to create an
 * SSLContext, which can be used to open a TLS/SSL session.
 */
public class SSLTransportParameters 
{
    public static final String param_keystore           = "keystore";
    public static final String param_keystorepassword   = "keystorepassword";
    public static final String param_truststore         = "truststore";
    public static final String param_truststorepassword = "truststorepassword";
  
    private String protocol = "TLS";
    private String keyStore;
    private String keyPass;
    private String keyManagerType = "SunX509";
    private String keyStoreType = "JKS";
    private String trustStore;
    private String trustPass;
    private String trustManagerType = "SunX509";
    private String trustStoreType = "JKS";
    private boolean isKeyStoreSet = false;
    private boolean isTrustStoreSet = false;

    /**
     * Create a new SSLTransportParameters object with all default values.
     */
    public SSLTransportParameters() {}

    /**
     * Create an SSLContext based on the truststore and/or keystore parameters
     * defined for this obejct. An exception is thrown if any paramters are invalid.
     * 
     * @return  New SSLContext based on the parameters in this object.
     * @throws  Exception   If any parameters are invalid.
     */
    public SSLContext createSSLContext() throws Exception 
    {
        SSLContext ctx = SSLContext.getInstance(this.protocol);
        TrustManagerFactory tmf = null;
        KeyManagerFactory kmf = null;

        if (this.isTrustStoreSet) 
        {
            tmf = TrustManagerFactory.getInstance(this.trustManagerType);
            KeyStore ts = KeyStore.getInstance(this.trustStoreType);
            ts.load(new FileInputStream(this.trustStore), this.trustPass.toCharArray());
            tmf.init(ts);
        }

        if (this.isKeyStoreSet) 
        {
            kmf = KeyManagerFactory.getInstance(this.keyManagerType);
            KeyStore ks = KeyStore.getInstance(this.keyStoreType);
            ks.load(new FileInputStream(this.keyStore), this.keyPass.toCharArray());
            kmf.init(ks, this.keyPass.toCharArray());
        }

        if (this.isKeyStoreSet && this.isTrustStoreSet)
        {
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        }
        else if (this.isKeyStoreSet) 
        {
            ctx.init(kmf.getKeyManagers(), null, null);
        }
        else
        {
            ctx.init(null, tmf.getTrustManagers(), null);
        }
        return ctx;
    }

    /**
     * Whether KeyStore set defined
     */
    public boolean isKeyStoreSet() {
        return isKeyStoreSet;
    }

    /**
     * Whether TrustStore set defined
     */
    public boolean isTrustStoreSet() {
        return isTrustStoreSet;
    }
    
    /**
     * Get path to TrustStore file
     * @return Path to TrustStore file
     */
    public String trustStore() {
        return trustStore;
    }
    
    /**
     * Get TrustStore password
     * @return TrustStore Password
     */
    public String trustPass() {
        return trustPass;
    }
    
    /**
     * Get TrustManager type
     * @return TrustManager type
     */
    public String trustManagerType() {
        return trustManagerType;
    }

    /**
     * Get TrustStore type
     * @return TrustStore type
     */
    public String trustStoreType() {
        return trustStoreType;
    }

    /**
     * Get path to KeyStore file
     * @return Path to KeyStore file
     */
    public String keyStore() {
        return keyStore;
    }
    
    /**
     * Get KeyStore password
     * @return KeyStore Password
     */
    public String keyPass() {
        return keyPass;
    }
    
    /**
     * Get KeyManager type
     * @return KeyManager type
     */
    public String keyManagerType() {
        return keyManagerType;
    }

    /**
     * Get KeyStore type
     * @return KeyStore type
     */
    public String keyStoreType() {
        return keyStoreType;
    }

    /**
     * Set the keystore, password, certificate type and the store type
     * 
     * @param keyStore Location of the Keystore on disk
     * @param keyPass Keystore password
     * @param keyManagerType The default is X509
     * @param keyStoreType The default is JKS
     */
    public void setKeyStore(String keyStore, String keyPass, String keyManagerType, String keyStoreType) 
    {
        if((keyStore == null) || (keyPass == null))
        {
            this.keyStore =  System.getProperty("javax.net.ssl.keyStore");
            this.keyPass = System.getProperty("javax.net.ssl.keyStorePassword");
        }
        else
        {
            this.keyStore = keyStore;
            this.keyPass = keyPass;
        }
        
        if (keyManagerType != null) {
            this.keyManagerType = keyManagerType;
        }
        if (keyStoreType != null) {
            this.keyStoreType = keyStoreType;
        }
        isKeyStoreSet = (keyStore != null) && (keyPass != null);
    }
    
    /**
     * Set the keystore and password
     * 
     * @param keyStore Location of the Keystore on disk
     * @param keyPass Keystore password
     */
    public void setKeyStore(String keyStore, String keyPass) 
    {
        setKeyStore(keyStore, keyPass, null, null);
    }
    
    /**
     * Set the truststore, password, certificate type and the store type
     * 
     * @param trustStore Location of the Truststore on disk
     * @param trustPass Truststore password
     * @param trustManagerType The default is X509
     * @param trustStoreType The default is JKS
     */
    public void setTrustStore(String trustStore, String trustPass, String trustManagerType, String trustStoreType) 
    {
        if((trustStore == null) || (trustPass == null))
        {
            this.trustStore =  System.getProperty("javax.net.ssl.trustStore");
            this.trustPass =  System.getProperty("javax.net.ssl.trustStorePassword");
        }
        else
        {
            this.trustStore = trustStore;
            this.trustPass = trustPass;
        }
 
        if (trustManagerType != null) {
            this.trustManagerType = trustManagerType;
        }
        if (trustStoreType != null) {
            this.trustStoreType = trustStoreType;
        }
        isTrustStoreSet = (trustStore != null) && (trustPass != null);
    }
    
    /**
     * Set the truststore and password
     * 
     * @param trustStore    Location of the Truststore on disk
     * @param trustPass     Truststore password
     */
    public void setTrustStore(String trustStore, String trustPass) {
        setTrustStore(trustStore, trustPass, null, null);
    }
    
}   // class SSLTransportParameters
