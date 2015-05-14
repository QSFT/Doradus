package com.dell.doradus.logservice;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class ChunkReader {
    private int m_size;
    private long[] m_timestamps;
    private MemoryStream m_data = new MemoryStream();
    private ArrayList<BSTR> m_fieldNames = new ArrayList<>();
    private ArrayList<int[]> m_offsets = new ArrayList<>();
    private ArrayList<int[]> m_lengths = new ArrayList<>();
    private HashMap<BSTR, Integer> m_fieldsMap = new HashMap<>();
    
    public ChunkReader() {}
    
    public void read(byte[] data) {
        m_data.clear();
        m_fieldNames.clear();
        m_offsets.clear();
        m_lengths.clear();
        m_fieldsMap.clear();
        MemoryStream input = new MemoryStream(data);
        byte version = (byte)input.readByte();
        if(version != 1) throw new RuntimeException("Unknown format");
        m_size = input.readVInt();
        int fieldsCount = input.readVInt();
        //timestamps;
        m_timestamps = new long[m_size];
        MemoryStream s_ts = Temp.readCompressed(input);
        long last = 0;
        for(int i = 0; i < m_size; i++) {
            last += s_ts.readVLong();
            m_timestamps[i] = last;
        }
        //fields
        for(int f = 0; f < fieldsCount; f++) {
            BSTR field = input.readString();
            int valuesCount = input.readVInt();
            int[] positions = new int[m_size];
            int[] lengths = new int[m_size];
            
            m_fieldNames.add(field);
            m_offsets.add(positions);
            m_lengths.add(lengths);
            m_fieldsMap.put(field, new Integer(f));
            
            MemoryStream s_pfx = Temp.readCompressed(input);
            MemoryStream s_sfx = Temp.readCompressed(input);
            MemoryStream s_len = Temp.readCompressed(input);
            MemoryStream s_dat = Temp.readCompressed(input);
            
            int lastPos = m_data.position();
            int lastLen = 0;
            int[] offsets = new int[valuesCount];
            int[] lens = new int[valuesCount];
            for(int i = 0; i < valuesCount; i++) {
                int pfx = s_pfx.readVInt();
                int sfx = s_sfx.readVInt();
                int len = s_len.readVInt();
                offsets[i] = m_data.position();
                lens[i] = len;
                int ifx = len - sfx - pfx;
                if(pfx > 0) m_data.write(m_data.getBuffer(), lastPos, pfx);
                if(ifx > 0) {
                    m_data.write(s_dat.getBuffer(), s_dat.position(), ifx);
                    s_dat.skip(ifx);
                }
                if(sfx > 0) m_data.write(m_data.getBuffer(), lastPos + lastLen - sfx, sfx);
                lastPos = offsets[i];
                lastLen = len;
            }
            
            if(valuesCount >= 256) {
                MemoryStream s_lst = Temp.readCompressed(input);
                MemoryStream s_fst = Temp.readCompressed(input);
                for(int i = 0; i < m_size; i++) {
                    int n = (s_fst.readVInt() << 8) + s_lst.readByte();
                    positions[i] = offsets[n];
                    lengths[i] = lens[n];
                }
            } else {
                MemoryStream s_lst = Temp.readCompressed(input);
                for(int i = 0; i < m_size; i++) {
                    int n = s_lst.readByte();
                    positions[i] = offsets[n];
                    lengths[i] = lens[n];
                }
            }
        }
        
        if(!input.end()) throw new RuntimeException("Unexpected data");
    }
    
    public int size() { return m_size; }
    public int fieldsCount() { return m_fieldNames.size(); }
    
    public long getTimestamp(int doc) { return m_timestamps[doc]; }
    public Date getDate(int doc) { return new Date(getTimestamp(doc)); }
    
    public ArrayList<BSTR> getFieldNames() { return m_fieldNames; }
    public int getFieldIndex(BSTR field) {
        Integer i = m_fieldsMap.get(field);
        if(i == null) return -1;
        else return i.intValue();
    }
    
    public void getFieldValue(int doc, int field, BSTR value) {
        value.length = 0;
        int[] offsets = m_offsets.get(field);
        int[] lengths = m_lengths.get(field);
        m_data.seek(offsets[doc]);
        int len = lengths[doc];
        value.assertLength(len);
        m_data.read(value.buffer, 0, len);
        value.length = len;
    }

    public void getFieldValue(int doc, int field, Str value) {
        value.clear();
        int[] offsets = m_offsets.get(field);
        int[] lengths = m_lengths.get(field);
        if(doc >= offsets.length || doc >= lengths.length) {
            System.out.println("AAA");
        }
        value.set(m_data.getBuffer(), offsets[doc], lengths[doc]);
    }

    public void load(int doc, LogRecord record) {
        record.setFieldsCount(fieldsCount());
        record.clear();
        record.setTimestamp(m_timestamps[doc]);
        for(int f = 0; f < fieldsCount(); f++) {
            Str value = record.fieldValue(f);
            getFieldValue(doc, f, value);
        }
    }
}
