package simpledb.query;

import simpledb.record.RID;

import java.util.List;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 *
 * @author Edward Sciore
 */
public class ProjectScan implements UpdateScan {
    private Scan s;
    private List<String> fieldlist;

    /**
     * Create a project scan having the specified
     * underlying scan and field list.
     *
     * @param s         the underlying scan
     * @param fieldlist the list of field names
     */
    public ProjectScan(Scan s, List<String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
    }

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        return s.next();
    }

    public int getInt(String fldname) {
        if (hasField(fldname))
            return s.getInt(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public String getString(String fldname) {
        if (hasField(fldname))
            return s.getString(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public Constant getVal(String fldname) {
        if (hasField(fldname))
            return s.getVal(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public boolean hasField(String fldname) {
        return fieldlist.contains(fldname);
    }

    public void close() {
        s.close();
    }

    // UpdateScan methods

    @Override
    public void setVal(String fldname, Constant val) {
        if (hasField(fldname)) {
            UpdateScan updateScan = (UpdateScan) s;
            updateScan.setVal(fldname, val);
        } else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    @Override
    public void setInt(String fldname, int val) {
        if (hasField(fldname)) {
            UpdateScan updateScan = (UpdateScan) s;
            updateScan.setInt(fldname, val);
        } else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    @Override
    public void setString(String fldname, String val) {
        if (hasField(fldname)) {
            UpdateScan updateScan = (UpdateScan) s;
            updateScan.setString(fldname, val);
        } else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    @Override
    public void insert() {
        UpdateScan us = (UpdateScan) s;
        us.insert();
    }

    @Override
    public void delete() {
        UpdateScan us = (UpdateScan) s;
        us.delete();
    }

    @Override
    public RID getRid() {
        UpdateScan us = (UpdateScan) s;
        return us.getRid();
    }

    @Override
    public void moveToRid(RID rid) {
        UpdateScan us = (UpdateScan) s;
        us.moveToRid(rid);
    }
}
