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

package com.dell.doradus.utilities;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import com.dell.doradus.common.Utils;

/**
 * Implements methods to get password hashes for storing,
 * using modern security considerations.
 */

public class PasswordManager {
    private static int ITERATIONS = 20000;
    private static final Random RANDOM = new SecureRandom();
    public static final String ALGORITHM = "PBKDF2WithHmacSHA1";
    public static final int HASH_LENGTH = 32;
    

    public static String hash(String password) {
        Utils.require(password.length() > 0, "Empty password is not allowed");
        byte[] salt = new byte[HASH_LENGTH];
        synchronized(RANDOM) {
            RANDOM.nextBytes(salt);
        }
        return getHash(password, salt, ITERATIONS);
    }
    
    public static boolean checkPassword(String password, String hash) {
        List<String> parts = Utils.split(hash, '-');
        if(!"a".equals(parts.get(0))) throw new RuntimeException("Invalid security format");
        if(parts.size() != 4) throw new RuntimeException("Invalid security format");
        int iterations = Integer.parseInt(parts.get(1));
        byte[] salt = Utils.hexToBinary(parts.get(2));
        String v_hash = getHash(password, salt, iterations);
        if(hash.hashCode() != v_hash.hashCode()) return false; // paranoid: to prevent "timing attack"
        return hash.equals(v_hash);
        
    }

    private static String getHash(String password, byte[] salt, int iterations) {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, HASH_LENGTH * 8);
        try {
            SecretKeyFactory skf = SecretKeyFactory.getInstance(ALGORITHM);
            byte[] hash = skf.generateSecret(spec).getEncoded();
            return "a-" + ITERATIONS + "-" + Utils.toHexBytes(salt) + "-" + Utils.toHexBytes(hash);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Security error", e);
        }
        
    }
    

}
