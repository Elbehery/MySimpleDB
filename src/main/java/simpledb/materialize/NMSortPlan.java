package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.List;

public class NMSortPlan implements Plan {
    private Transaction tx;
    private Plan plan;
    private Schema schema;
    private RecordComparator recordComparator;

    public NMSortPlan(Transaction tx, Plan plan, List<String> sortFields) {
        this.tx = tx;
        this.plan = plan;
        this.schema = plan.schema();
        this.recordComparator = new RecordComparator(sortFields);
    }

    @Override
    public Scan open() {
        UpdateScan scan = (UpdateScan) plan.open();
        return new NMSortScan(scan, recordComparator);
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
        return plan.distinctValues();
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }
}
