package mysimpledb.record;


import mysimpledb.file.Page;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;

public class MyLayout {

    private MySchema schema;
    private Map<String, Integer> offsets;
    private int slotSize;

    public MyLayout(MySchema schema) {
        this.schema = schema;
        this.offsets = new HashMap<>();
        int pos = Integer.BYTES;
        for (String field : schema.fields()) {
            offsets.put(field, pos);
            pos += lengthInBytes(field);
        }
        slotSize = pos;
    }

    public MySchema getSchema() {
        return schema;
    }

    public int offset(String fieldName) {
        return offsets.get(fieldName);
    }

    public int slotSize() {
        return slotSize;
    }

    public MyLayout(MySchema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    private int lengthInBytes(String fieldName) {
        int type = schema.type(fieldName);
        switch (type) {
            case INTEGER:
                return Integer.BYTES;
            case VARCHAR:
                return Page.maxLength(schema.length(fieldName));
            default:
                throw new IllegalArgumentException();
        }
    }
}
