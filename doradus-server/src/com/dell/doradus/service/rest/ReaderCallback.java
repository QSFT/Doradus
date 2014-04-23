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

package com.dell.doradus.service.rest;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;

/**
 * Provides a {@link RESTCallback} variant interface that allows the input entity to be
 * passed as a {@link Reader} while returning the command response as a {@link RESTResponse}.
 * The subclass must implement {@link #invokeStreamIn(Reader)}, which is called when the
 * callback is invoked.
 */
public abstract class ReaderCallback extends RESTCallback {

    @Override
    public final RESTResponse invoke() {
        InputStream inStream = m_request.getInputStream();
        Reader reader = null;
        if (inStream != null) {
            reader = new InputStreamReader(inStream, Utils.UTF8_CHARSET);
        }
        return invokeStreamIn(reader);
    }   // invoke

    /**
     * The subclass must implement this method, processing the REST request with the given
     * parameters.
     * 
     * @param reader        Input entity passed as a {@link Reader} object. If there is no
     *                      input entity, this parameter will be null. Otherwise, as it is
     *                      reader, the reader will decompress the input entity if needed
     *                      and convert it from binary to characters using UTF-8.
     * @return              {@link RESTResponse} to be returned to the client.
     */
    public abstract RESTResponse invokeStreamIn(Reader reader);
    
}   // class ReaderCallback

