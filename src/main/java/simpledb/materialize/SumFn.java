package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class SumFn implements AggregationFn {
    private String fieldName;
    private int sum;

    public SumFn(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void processFirst(Scan s) {
        sum = s.getInt(fieldName);
    }

    @Override
    public void processNext(Scan s) {
        sum += s.getInt(fieldName);
    }

    @Override
    public String fieldName() {
        return "sumof" + fieldName;
    }

    @Override
    public Constant value() {
        return new Constant(sum);
    }
}
