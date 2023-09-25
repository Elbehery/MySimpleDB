package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class GBNoDupsPlan implements Plan {
    private Plan plan;
    private List<String> sortFields;
    private Schema schema;

    public GBNoDupsPlan(Transaction tx, Plan plan, List<String> sortFields) {
        this.plan = new GroupByPlan(tx, plan, sortFields, Collections.EMPTY_LIST);
        this.sortFields = sortFields;
        this.schema = plan.schema();
    }

    @Override
    public Scan open() {
        Scan scan = plan.open();
        return new GroupByScan(scan, sortFields, Collections.EMPTY_LIST);
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
        return schema;
    }
}
