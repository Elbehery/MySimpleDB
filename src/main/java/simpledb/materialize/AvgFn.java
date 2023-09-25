package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn {
    private String fieldName;
    private int count;
    private int sum;

    public AvgFn(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void processFirst(Scan s) {
        sum = s.getInt(fieldName);
        count++;
    }

    @Override
    public void processNext(Scan s) {
        sum += s.getInt(fieldName);
        count++;
    }

    @Override
    public String fieldName() {
        return "avgof" + fieldName;
    }

    @Override
    public Constant value() {
        return new Constant(sum / count);
    }
}
