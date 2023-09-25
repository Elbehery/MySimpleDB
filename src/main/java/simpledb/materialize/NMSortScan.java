package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NMSortScan implements Scan {
    private UpdateScan scan;
    private RecordComparator recordComparator;
    private Record current, lastInOrder;
    private Set<RID> seen;

    public NMSortScan(UpdateScan scan, RecordComparator recordComparator) {
        this.scan = scan;
        this.recordComparator = recordComparator;
        this.seen = new HashSet<>();
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        beforeFirst();
        while (scan.next()) {
            // skip records retrieved in order beforehand
            if (seen.contains(scan.getRid())) {
                continue;
            }
            // only initial scan to be compared against current
            if (lastInOrder == null) {
                lastInOrder = new Record(scan.getRid());
                for (String field : recordComparator.getFields()) {
                    lastInOrder.updateSortValue(field, scan.getVal(field));
                }
            }

            // populate current record with current scan values
            current = new Record(scan.getRid());
            for (String field : recordComparator.getFields()) {
                lastInOrder.updateSortValue(field, scan.getVal(field));
            }

            // update lastInOrder if current is earlier in order
            if (recordComparator.compare(current, lastInOrder) < 0) {
                lastInOrder = new Record(current.getRid());
                for (String field : recordComparator.getFields()) {
                    lastInOrder.updateSortValue(field, current.getVal(field));
                }
            }
        }
        if (seen.contains(lastInOrder.rid))
            return false;
        seen.add(lastInOrder.rid);
        return true;
    }

    @Override
    public int getInt(String fldname) {
        return scan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return scan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return scan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return scan.hasField(fldname);
    }

    @Override
    public void close() {
        scan.close();
    }

    private class Record implements Scan {
        private RID rid;
        private Map<String, Constant> sortValues;

        private Record(RID rid) {
            this.rid = rid;
            this.sortValues = new HashMap<>();
        }

        public RID getRid() {
            return rid;
        }

        public void setRid(RID rid) {
            this.rid = rid;
        }

        public void updateSortValue(String fieldName, Constant val) {
            sortValues.put(fieldName, val);
        }

        @Override
        public void beforeFirst() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(String fldname) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getString(String fldname) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Constant getVal(String fldname) {
            return sortValues.get(fldname);
        }

        @Override
        public boolean hasField(String fldname) {
            return sortValues.containsKey(fldname);
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}
