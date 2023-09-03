package simpledb.query;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class ExtendScan implements Scan {

    private TableScan scan;
    private Expression expression;
    private String newFieldName;

    public ExtendScan(Scan scan, Expression expression, String newFieldName) {
        if (scan == null)
            throw new NullPointerException("input table can not be null");

        if (expression == null)
            throw new NullPointerException("input expression can not be null");

        if (newFieldName == null)
            throw new NullPointerException("input field name can not be null");

        if (newFieldName == "")
            throw new IllegalArgumentException("input field name can not be empty string");

        if (!expression.isFieldName())
            throw new IllegalArgumentException("input expression must contain a field name");

        // validate the expression against the input table
        TableScan tableScan = (TableScan) scan;
        Layout tableLayout = tableScan.getLayout();
        if (!tableLayout.schema().hasField(expression.asFieldName()))
            throw new IllegalArgumentException("input expression must contain an existing field name within the input table's schema");

        this.scan = tableScan;
        this.expression = expression;
        this.newFieldName = newFieldName;

        // add the new field to the scan's schema, only on the fly, not in the catalog
        updateSchema(expression.asFieldName(), newFieldName);
    }

    @Override
    public void beforeFirst() {
        this.scan.beforeFirst();
    }

    @Override
    public boolean next() {
        return this.scan.next();
    }

    @Override
    public int getInt(String fldname) {
        if (fldname.equals(newFieldName)) {
            Constant val = expression.evaluate(scan);
            return val.asInt();
        }
        return this.scan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (fldname.equals(newFieldName)) {
            Constant val = expression.evaluate(scan);
            return val.asString();
        }
        return this.scan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (fldname.equals(newFieldName)) {
            Constant val = expression.evaluate(scan);
            return val;
        }
        return this.scan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return newFieldName.equals(fldname) || this.scan.hasField(fldname);
    }

    @Override
    public void close() {
        this.scan.close();
    }

    private void updateSchema(String oldFieldName, String newFieldName) {
        Layout layout = scan.getLayout();
        Schema oldSchema = layout.schema();
        Schema newSchema = new Schema();
        newSchema.addAll(oldSchema);

        // add the new field to the schema
        int type = oldSchema.type(oldFieldName);
        int length = oldSchema.length(oldFieldName);
        newSchema.addField(newFieldName, type, length);
        layout = new Layout(newSchema);
        this.scan.setLayout(layout);
    }
}
