package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MySortPlan implements Plan {

    private Plan plan;
    private Transaction tx;
    private RecordComparator recordComparator;
    private Schema schema;

    public MySortPlan(Plan plan, Transaction tx, List<String> sortFields) {
        this.plan = plan;
        this.tx = tx;
        this.schema = plan.schema();
        this.recordComparator = new RecordComparator(sortFields);
    }

    @Override
    public Scan open() {
        Scan src = plan.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();

        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new SortScan(runs, recordComparator);
    }

    @Override
    public int blocksAccessed() {
        return 0;
    }

    @Override
    public int recordsOutput() {
        return 0;
    }

    @Override
    public int distinctValues(String fldname) {
        return 0;
    }

    @Override
    public Schema schema() {
        return null;
    }


    private List<TempTable> splitIntoRuns(Scan src) {
        src.beforeFirst();
        if (!src.next()) {
            return Collections.emptyList();
        }

        List<TempTable> runs = new ArrayList<>();
        TempTable currentRun = new TempTable(tx, schema);
        runs.add(currentRun);
        UpdateScan dest = currentRun.open();

        while (copy(src, dest)) {
            if (recordComparator.compare(src, dest) < 0) {
                // start a new run
                dest.close();
                currentRun = new TempTable(tx, schema);
                runs.add(currentRun);
                dest = currentRun.open();
            }
        }

        dest.close();
        return runs;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable t1 = runs.remove(0);
            TempTable t2 = runs.remove(1);
            result.add(mergeTwoRuns(t1, t2));
        }

        if (runs.size() == 1) {
            result.add(runs.remove(0));
        }

        return result;
    }

    private TempTable mergeTwoRuns(TempTable t1, TempTable t2) {
        Scan s1 = t1.open();
        s1.beforeFirst();
        boolean hasMore1 = s1.next();
        Scan s2 = t2.open();
        s2.beforeFirst();
        boolean hasMore2 = s2.next();

        TempTable merged = new TempTable(tx, schema);
        UpdateScan dest = merged.open();

        while (hasMore1 && hasMore2) {
            if (recordComparator.compare(s1, s2) < 0) {
                hasMore1 = copy(s1, dest);
            } else {
                hasMore2 = copy(s2, dest);
            }
        }

        if (hasMore1) {
            while (hasMore1) {
                hasMore1 = copy(s1, dest);
            }
        } else {
            while (hasMore2) {
                hasMore2 = copy(s2, dest);
            }
        }

        s1.close();
        s2.close();
        dest.close();

        return merged;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String field : schema.fields())
            dest.setVal(field, src.getVal(field));

        return src.next();
    }
}
