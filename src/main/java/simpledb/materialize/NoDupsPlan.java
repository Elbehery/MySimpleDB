package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.List;

public class NoDupsPlan implements Plan {
    private Plan p;
    private List<String> sortFields;
    private Schema sch;

    public NoDupsPlan(Transaction tx, Plan p, List<String> sortFields) {
        this.p = new SortPlan(tx, p, sortFields);
        this.sortFields = sortFields;
        this.sch = p.schema();
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new NoDupsScan(s, sortFields);
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
