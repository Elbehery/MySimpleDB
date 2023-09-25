package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.List;

public class NoDupsScan implements Scan {
    private Scan scan;
    private List<String> sortFields;
    private GroupValue groupValue;
    private boolean moreGroups;

    public NoDupsScan(Scan scan, List<String> sortFields) {
        this.scan = scan;
        this.sortFields = sortFields;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
        moreGroups = scan.next();
    }

    @Override
    public boolean next() {
        if (!moreGroups)
            return false;
        groupValue = new GroupValue(scan, sortFields);
        while (moreGroups = scan.next()) {
            GroupValue gv = new GroupValue(scan, sortFields);
            if (!groupValue.equals(gv)) {
                break;
            }
        }
        return true;
    }

    @Override
    public int getInt(String fldname) {
        if (sortFields.contains(fldname)) {
            return groupValue.getVal(fldname).asInt();
        }
        throw new RuntimeException("field " + fldname + " not found.");
    }

    @Override
    public String getString(String fldname) {
        if (sortFields.contains(fldname)) {
            return groupValue.getVal(fldname).asString();
        }
        throw new RuntimeException("field " + fldname + " not found.");
    }

    @Override
    public Constant getVal(String fldname) {
        if (sortFields.contains(fldname)) {
            return groupValue.getVal(fldname);
        }
        throw new RuntimeException("field " + fldname + " not found.");
    }

    @Override
    public boolean hasField(String fldname) {
        return sortFields.contains(fldname);
    }

    @Override
    public void close() {
        scan.close();
    }
}
