package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

import java.util.List;

public class MySortScan implements Scan {

    private UpdateScan s1, s2, currentScan;
    private boolean hasMore1, hasMore2;
    private RecordComparator comparator;
    private List<RID> savedPositions;

    public MySortScan(List<TempTable> runs, RecordComparator comparator) {
        this.comparator = comparator;
        this.s1 = runs.get(0).open();
        if (runs.size() > 1)
            this.s2 = runs.get(1).open();
    }

    @Override
    public void beforeFirst() {
        currentScan = null;
        s1.beforeFirst();
        hasMore1 = s1.next();
        if (s2 != null) {
            s2.beforeFirst();
            hasMore2 = s2.next();
        }
    }

    @Override
    public boolean next() {
        if (currentScan != null) {
            if (currentScan == s1) {
                hasMore1 = s1.next();
            } else if (currentScan == s2) {
                hasMore2 = s2.next();
            }
        }

        if (!hasMore1 && !hasMore2) {
            return false;
        } else if (hasMore1 && hasMore2) {
            if (comparator.compare(s1, s2) < 0)
                currentScan = s1;
            else
                currentScan = s2;
        } else if (hasMore1) {
            currentScan = s1;
        } else
            currentScan = s2;

        return true;
    }

    @Override
    public int getInt(String fldname) {
        return currentScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return null;
    }

    @Override
    public Constant getVal(String fldname) {
        return null;
    }

    @Override
    public boolean hasField(String fldname) {
        return false;
    }

    @Override
    public void close() {
        s1.close();
        if (s2 != null)
            s2.close();
    }

    public void savePositions() {
        savedPositions.add(s1.getRid());
        if (s2 != null)
            savedPositions.add(s2.getRid());
    }

    public void restorePositions() {
        s1.moveToRid(savedPositions.get(0));
        if (savedPositions.size() > 1)
            s2.moveToRid(savedPositions.get(1));
    }
}
