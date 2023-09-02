package simpledb.query;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class RenameScan implements Scan {
    private TableScan tableScan;

    public RenameScan(TableScan tableScan, String oldFieldName, String newFieldName) {
        if (tableScan == null)
            throw new NullPointerException("input table scan can not be null");

        if (!tableScan.getLayout().schema().hasField(oldFieldName))
            throw new IllegalArgumentException("input oldFieldName must be in the input table's schema");

        this.tableScan = tableScan;
        updateSchema(oldFieldName, newFieldName);
    }

    @Override
    public void beforeFirst() {
        this.tableScan.beforeFirst();
    }

    @Override
    public boolean next() {
        return this.tableScan.next();
    }

    @Override
    public int getInt(String fldname) {
        return this.tableScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return this.tableScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return this.tableScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return this.tableScan.hasField(fldname);
    }

    @Override
    public void close() {
        this.tableScan.close();
    }

    private void updateSchema(String oldFieldName, String newFieldName) {
        Layout layout = tableScan.getLayout();
        Schema oldSchema = layout.schema();
        Schema newSchema = new Schema();
        for (String fld : oldSchema.fields()) {
            if (fld.equals(oldFieldName)) {
                int type = oldSchema.type(oldFieldName);
                int length = oldSchema.length(oldFieldName);
                newSchema.addField(newFieldName, type, length);
            } else {
                newSchema.add(fld, oldSchema);
            }
        }
        layout = new Layout(newSchema);
        this.tableScan.setLayout(layout);
    }
}
