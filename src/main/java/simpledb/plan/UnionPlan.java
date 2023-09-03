package simpledb.plan;

import simpledb.query.Scan;
import simpledb.query.UnionScan;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class UnionPlan implements Plan {

    private Plan p1, p2, currentPlan;

    public UnionPlan(Plan p1, Plan p2) {
        if (!validateSchema(p1, p2))
            throw new IllegalArgumentException("input plans must have identical schema");
        this.p1 = p1;
        this.p2 = p2;
    }

    @Override
    public Scan open() {
        Scan s = new UnionScan((TableScan) p1.open(), (TableScan) p2.open());
        return s;
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() + p2.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return (p1.schema().hasField(fldname)) ? p1.distinctValues(fldname) : p2.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        Schema schema = new Schema();
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
        return schema;
    }

    private boolean validateSchema(Plan p1, Plan p2) {
        Schema schema1 = p1.schema();
        Schema schema2 = p2.schema();

        for (String fld : schema1.fields()) {
            if (!schema2.hasField(fld)) {
                return false;
            }
            if (schema1.type(fld) != schema2.type(fld) || schema1.length(fld) != schema2.length(fld)) {
                return false;
            }
        }
        return true;
    }
}
