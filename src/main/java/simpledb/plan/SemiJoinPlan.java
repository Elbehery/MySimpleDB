package simpledb.plan;

import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.SemiJoin;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class SemiJoinPlan implements Plan {

    private Plan p1, p2;

    private Predicate predicate;

    private ProductPlan productPlan;

    public SemiJoinPlan(Plan p1, Plan p2, Predicate predicate) {
        this.p1 = p1;
        this.p2 = p2;
        this.predicate = predicate;
        this.productPlan = new ProductPlan(p1, p2);
    }

    @Override
    public Scan open() {
        return new SemiJoin((TableScan) p1.open(), (TableScan) p2.open(), predicate);
    }

    @Override
    public int blocksAccessed() {
        return productPlan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return productPlan.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return productPlan.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return productPlan.schema();
    }
}
