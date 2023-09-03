package simpledb.query;

import simpledb.record.TableScan;

public class SemiJoin implements Scan {
    private TableScan s1, s2;
    private Predicate predicate;

    private ProductScan productScan;

    public SemiJoin(TableScan s1, TableScan s2, Predicate predicate) {
        this.s1 = s1;
        this.s2 = s2;
        this.predicate = predicate;
        this.productScan = new ProductScan(s1, s2);
    }

    @Override
    public void beforeFirst() {
        productScan.beforeFirst();
    }

    @Override
    public boolean next() {
        boolean hasNext = productScan.next();
        while (!predicate.isSatisfied(productScan) && hasNext) {
            hasNext = productScan.next();
        }
        if (!hasNext)
            return false;

        return true;
    }

    @Override
    public int getInt(String fldname) {
        return productScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return productScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return productScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return productScan.hasField(fldname);
    }

    @Override
    public void close() {
        productScan.close();
    }
}
