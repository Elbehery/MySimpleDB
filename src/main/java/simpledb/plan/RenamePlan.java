package simpledb.plan;

import simpledb.query.RenameScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class RenamePlan implements Plan {

    private TablePlan plan;

    private String oldFieldName, newFieldName;

    public RenamePlan(TablePlan plan, String oldFieldName, String newFieldName) {
        this.plan = plan;
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;
        updateSchema(oldFieldName, newFieldName);
    }

    @Override
    public Scan open() {
        TableScan s = (TableScan) plan.open();
        return new RenameScan(s, oldFieldName, newFieldName);
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return plan.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }

    private void updateSchema(String oldFieldName, String newFieldName) {
        Schema oldSchema = schema();
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
        Layout layout = new Layout(newSchema);
        this.plan.setLayout(layout);
    }
}
