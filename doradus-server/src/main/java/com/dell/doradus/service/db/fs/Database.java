package com.dell.doradus.service.db.fs;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.olap.io.BSTR;

public class Database {
    private SeekableByteChannel m_channel;
    private FInputStream m_input;
    private FOutputStream m_output;
    
    public Database(String fileName) {
        try {
            Set<OpenOption> options = new HashSet<OpenOption>();
            options.add(StandardOpenOption.CREATE);
            options.add(StandardOpenOption.READ);
            options.add(StandardOpenOption.WRITE);
            m_channel = Files.newByteChannel(Paths.get(fileName), options);
            m_input = new FInputStream(m_channel);
            m_output = new FOutputStream(m_channel);
        }catch(IOException e) { throw new RuntimeException(e); }
    }
    
    public void close() {
        try {
            m_channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public long position() {
        return m_input.position();
    }

    public long length() {
        return m_output.length();
    }
    
    public BSTR read(long position) {
        m_input.seek(position);
        BSTR value = m_input.readBSTR();
        return value;
    }

    
    public int readNextFlag() {
        return m_input.readVInt();
    }
    
    public BSTR readNext() {
        BSTR value = m_input.readBSTR();
        return value;
    }
    
    public void writeFlag(int flag) {
        m_output.writeVInt(flag);
    }
    public void write(BSTR value) {
        m_output.write(value);
    }
    public void flush() {
        m_output.flush();
        m_input.refresh();
    }
    
    public boolean isEnd() { return m_input.isEnd(); }
}
