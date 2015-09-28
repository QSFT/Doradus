package com.dell.doradus.service.db.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileUtils {
    public static final byte[] EMPTY_BYTES = new byte[0];
    
    public static void deleteDirectory(File dir) {
        if(!dir.exists()) return;
        for(File file: dir.listFiles()) {
            if(file.isDirectory()) deleteDirectory(file);
            else file.delete();
        }
        dir.delete();
    }

    public static String encode(String name) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if(c != '_' && Character.isLetterOrDigit(c)) sb.append(c);
            else {
                String esc = String.format("_%02x", (int)c);
                sb.append(esc);
            }
        }
        return sb.toString();
    }
    
    public static String decode(String name) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if(c != '_') sb.append(c);
            else {
                c = (char)Integer.parseInt(name.substring(i + 1, i + 3), 16);
                sb.append(c);
                i += 2;
            }
        }
        return sb.toString();
    }
    
    public static void write(File file, byte[] data) {
        try (FileOutputStream stream = new FileOutputStream(file)) {
            stream.write(data);
            stream.getChannel().force(true);
            stream.close();
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void append(File file, byte[] data) {
        try (FileOutputStream stream = new FileOutputStream(file, true)) {
            stream.write(data);
            stream.getChannel().force(true);
            stream.close();
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static byte[] read(File file) {
        if(!file.exists()) return null;
        try (FileInputStream stream = new FileInputStream(file)) {
            byte[] data = new byte[(int)file.length()];
            int len = stream.read(data);
            stream.close();
            if(len != data.length) throw new RuntimeException("Error reading file: " + file.getName());
            return data;
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
