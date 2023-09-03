package simpledb.query;

import simpledb.record.Schema;
import simpledb.record.TableScan;

public class UnionScan implements Scan {

    private TableScan s1, s2, currentScan;

    public UnionScan(TableScan s1, TableScan s2) {
        if (!validateSchema(s1, s2))
            throw new IllegalArgumentException("input tables must have identical schema");

        this.s1 = s1;
        this.s2 = s2;
    }

    @Override
    public void beforeFirst() {
        this.s1.beforeFirst();
        this.s2.beforeFirst();
    }

    @Override
    public boolean next() {
        if (s1.next())
            currentScan = s1;
        else if (s2.next())
            currentScan = s2;
        else
            return false;
        return true;
    }

    @Override
    public int getInt(String fldname) {
        return currentScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return currentScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return currentScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return currentScan.hasField(fldname);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    private boolean validateSchema(TableScan s1, TableScan s2) {
        Schema schema1 = s1.getLayout().schema();
        Schema schema2 = s2.getLayout().schema();

        for (String fld : schema1.fields()) {
            if (!schema2.hasField(fld)) {
                return false;
            }
            if (schema1.type(fld) != schema2.type(fld) || schema1.length(fld) != schema2.length(fld)) {
                return false;
            }
        }
        return true;
    }
}
