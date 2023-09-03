package simpledb.plan;

import simpledb.query.Expression;
import simpledb.query.ExtendScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class ExtendPlan implements Plan {

    private Plan plan;
    private Expression expression;
    private String newFieldName;

    public ExtendPlan(Plan plan, Expression expression, String newFieldName) {
        this.plan = plan;
        this.expression = expression;
        this.newFieldName = newFieldName;
    }

    @Override
    public Scan open() {
        Scan s = plan.open();
        return new ExtendScan(s, expression, newFieldName);
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
}
