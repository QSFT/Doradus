package com.dell.doradus.logservice;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.search.StrRef;
import com.dell.doradus.logservice.store.Temp;
import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.IntList;

public class ChunkReader {
    private int m_size;
    private long[] m_timestamps;
    private MemoryStream m_data = new MemoryStream();
    private MemoryStream m_input = new MemoryStream();
    private ArrayList<BSTR> m_fieldNames = new ArrayList<>();
    private IntList m_fieldOffsets = new IntList();
    private ArrayList<int[]> m_offsets = new ArrayList<>();
    private ArrayList<int[]> m_lengths = new ArrayList<>();
    private HashMap<BSTR, Integer> m_fieldsMap = new HashMap<>();
    
    public ChunkReader() {}
    
    public void read(byte[] data) {
        read(data, true);
    }
    
    public void read(byte[] data, boolean lazy) {
        m_data.clear();
        m_fieldNames.clear();
        m_fieldOffsets.clear();
        m_offsets.clear();
        m_lengths.clear();
        m_fieldsMap.clear();
        m_input = new MemoryStream(data);
        byte version = (byte)m_input.readByte();
        if(version != 1) throw new RuntimeException("Unknown format");
        m_size = m_input.readVInt();
        int fieldsCount = m_input.readVInt();
        //timestamps;
        m_timestamps = new long[m_size];
        MemoryStream s_ts = Temp.readCompressed(m_input);
        long last = 0;
        for(int i = 0; i < m_size; i++) {
            last += s_ts.readVLong();
            m_timestamps[i] = last;
        }
        //fields
        for(int f = 0; f < fieldsCount; f++) {
            BSTR field = new BSTR(m_input.readString());
            m_fieldNames.add(field);
            m_fieldsMap.put(field, new Integer(f));
            m_fieldOffsets.add(m_input.position());
            m_offsets.add(null);
            m_lengths.add(null);
            if(lazy) {
                skipField();
            } else {
                readField(f);
            }
        }
        
        if(!m_input.end()) throw new RuntimeException("Unexpected data");
    }
    
    private void skipField() {
        int valuesCount = m_input.readVInt();
        Temp.skipCompressed(m_input);
        Temp.skipCompressed(m_input);
        Temp.skipCompressed(m_input);
        Temp.skipCompressed(m_input);
        
        if(valuesCount >= 256) {
            Temp.skipCompressed(m_input);
            Temp.skipCompressed(m_input);
        } else {
            Temp.skipCompressed(m_input);
        }
    }
    
    private void readField(int fieldIndex) {
        m_input.seek(m_fieldOffsets.get(fieldIndex));
        int[] positions = new int[m_size];
        int[] lengths = new int[m_size];
        
        m_offsets.set(fieldIndex, positions);
        m_lengths.set(fieldIndex, lengths);
        
        int valuesCount = m_input.readVInt();
        MemoryStream s_pfx = Temp.readCompressed(m_input);
        MemoryStream s_sfx = Temp.readCompressed(m_input);
        MemoryStream s_len = Temp.readCompressed(m_input);
        MemoryStream s_dat = Temp.readCompressed(m_input);
        
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
            MemoryStream s_lst = Temp.readCompressed(m_input);
            MemoryStream s_fst = Temp.readCompressed(m_input);
            for(int i = 0; i < m_size; i++) {
                int n = (s_fst.readVInt() << 8) + s_lst.readByte();
                positions[i] = offsets[n];
                lengths[i] = lens[n];
            }
        } else {
            MemoryStream s_lst = Temp.readCompressed(m_input);
            for(int i = 0; i < m_size; i++) {
                int n = s_lst.readByte();
                positions[i] = offsets[n];
                lengths[i] = lens[n];
            }
        }
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
        if(offsets == null) {
            readField(field);
            offsets = m_offsets.get(field);
        }
        int[] lengths = m_lengths.get(field);
        m_data.seek(offsets[doc]);
        int len = lengths[doc];
        value.assertLength(len);
        m_data.read(value.buffer, 0, len);
        value.length = len;
    }

    public String getFieldValue(int doc, int field) {
        int[] offsets = m_offsets.get(field);
        if(offsets == null) {
            readField(field);
            offsets = m_offsets.get(field);
        }
        int[] lengths = m_lengths.get(field);
        return Utils.toString(m_data.getBuffer(), offsets[doc], lengths[doc]);
    }

    public void getFieldValue(int doc, int field, StrRef value) {
        int[] offsets = m_offsets.get(field);
        if(offsets == null) {
            readField(field);
            offsets = m_offsets.get(field);
        }
        int[] lengths = m_lengths.get(field);
        value.set(m_data.getBuffer(), offsets[doc], lengths[doc]);
    }
    
}
