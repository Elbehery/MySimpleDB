package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MyMergeJoinScan implements Scan {
    private Scan s1;
    private SortScan s2;
    private String fieldName1, fieldName2;
    private Constant joinValue = null;

    public MyMergeJoinScan(Scan s1, SortScan s2, String fieldName1, String fieldName2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fieldName1 = fieldName1;
        this.fieldName2 = fieldName2;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
    }

    @Override
    public boolean next() {
        boolean hasMore2 = s2.next();
        if (hasMore2 && s2.getVal(fieldName2).equals(joinValue)) {
            return true;
        }

        boolean hasMore1 = s1.next();
        if (hasMore1 && s1.getVal(fieldName1).equals(joinValue)) {
            s2.restorePosition();
            return true;
        }

        while (hasMore1 && hasMore2) {
            Constant val1 = s1.getVal(fieldName1);
            Constant val2 = s2.getVal(fieldName2);

            if (val1.compareTo(val2) < 0) {
                hasMore1 = s1.next();
            } else if (val1.compareTo(val2) > 0) {
                hasMore2 = s2.next();
            } else {
                s2.savePosition();
                joinValue = s2.getVal(fieldName2);
                return true;
            }
        }
        return false;
    }

    @Override
    public int getInt(String fldname) {
        return (s1.hasField(fldname) ? s1.getInt(fldname) : s2.getInt(fldname));
    }

    @Override
    public String getString(String fldname) {
        return (s1.hasField(fldname) ? s1.getString(fldname) : s2.getString(fldname));
    }

    @Override
    public Constant getVal(String fldname) {
        return (s1.hasField(fldname) ? s1.getVal(fldname) : s2.getVal(fldname));
    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}
