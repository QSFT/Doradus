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

package com.dell.doradus.service.db;

/**
 * Represents a "database not available" error which, when caught, is converted to a
 * "503 Service Unavailable" error in REST commands.
 */
public class DBNotAvailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DBNotAvailableException() {
    }

    public DBNotAvailableException(String message) {
        super(message);
    }

    public DBNotAvailableException(Throwable cause) {
        super(cause);
    }

    public DBNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBNotAvailableException(String message, Throwable cause,
                                   boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}   // class DBNotAvailableException
