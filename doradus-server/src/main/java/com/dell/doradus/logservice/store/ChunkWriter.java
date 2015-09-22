package com.dell.doradus.logservice.store;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.DateParser;
import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class ChunkWriter {
    private Temp m_temp;
    private Map<BSTR, FieldBuilder> m_fields;
    private TimestampBuilder m_timestamps;
    private MemoryStream m_output;
    private DateParser m_dateParser = new DateParser();
    
    public ChunkWriter() {
        m_temp = new Temp();
        m_fields = new LinkedHashMap<>();
        m_output = new MemoryStream();
    }
    
    public Set<BSTR> getFields() { return m_fields.keySet(); }
    public int getSize() { return m_timestamps.getSize(); }
    public long getMinTimestamp() { return m_timestamps.getMinTimestamp(); }
    public long getMaxTimestamp() { return m_timestamps.getMaxTimestamp(); }
    
    public void create(int size) {
        m_fields.clear();
        m_timestamps = new TimestampBuilder(size);
        m_output.clear();
    }

    public void setTimestamp(int doc, BSTR timestamp) {
        //long ts = Utils.parseDate(timestamp).getTimeInMillis();
        long ts = m_dateParser.getTimestamp(timestamp);
        m_timestamps.add(doc, ts);
    }

    public void setTimestamp(int doc, long timestamp) {
        m_timestamps.add(doc, timestamp);
    }
    
    public FieldBuilder getFieldBulider(BSTR field) {
        FieldBuilder fb = m_fields.get(field);
        if(fb == null) {
            Utils.require(FieldDefinition.isValidFieldName(field.toString()), "Field " + field + " is not valid");
            field = new BSTR(field);
            fb = new FieldBuilder(field, getSize());
            m_fields.put(field, fb);
        }
        return fb;
    }
    
    public byte[] getData() {
        int size = getSize();
        m_output.clear();
        m_output.writeByte((byte)1); // version
        m_output.writeVInt(size);
        m_output.writeVInt(m_fields.size());
        m_timestamps.flush(m_output, m_temp);
        for(FieldBuilder fb: m_fields.values()) {
            fb.flush(m_output, m_temp);
        }
        return m_output.toArray();
    }
    
}
