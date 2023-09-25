package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MinFn implements AggregationFn {
    private String fieldName;
    private Constant val;

    public MinFn(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void processFirst(Scan s) {
        val = s.getVal(fieldName);
    }

    @Override
    public void processNext(Scan s) {
        Constant newVal = s.getVal(fieldName);
        if (newVal.compareTo(val) < 0)
            val = newVal;
    }

    @Override
    public String fieldName() {
        return "minof" + fieldName;
    }

    @Override
    public Constant value() {
        return val;
    }
}
