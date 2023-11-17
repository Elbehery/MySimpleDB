package mysimpledb.record;

import simpledb.record.Schema;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.sql.Types.*;

public class MySchema {
    private List<String> fields = new LinkedList<>();
    private Map<String, FieldInfo> fieldInfo = new HashMap<>();

    public void addField(String fieldName, int type, int length) {
        FieldInfo info = new FieldInfo(type, length);
        fields.add(fieldName);
        fieldInfo.put(fieldName, info);
    }

    public void addIntField(String fieldName) {
        addField(fieldName, INTEGER, 0);
    }

    public void addStringField(String fieldName, int length) {
        addField(fieldName, VARCHAR, length);
    }

    public void add(String fieldName, Schema schema) {
        addField(fieldName, schema.type(fieldName), schema.length(fieldName));
    }

    public void addAll(Schema schema) {
        for (String field : schema.fields()) {
            add(field, schema);
        }
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String field) {
        return fieldInfo.containsKey(field);
    }

    public int type(String field) {
        return fieldInfo.get(field).type;
    }

    public int length(String field) {
        return fieldInfo.get(field).length;
    }

    class FieldInfo {
        int type, length;

        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}
