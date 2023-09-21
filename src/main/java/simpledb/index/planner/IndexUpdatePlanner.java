package simpledb.index.planner;

import java.util.*;

import com.sun.org.apache.bcel.internal.Const;
import simpledb.index.query.IndexSelectScan;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.metadata.*;
import simpledb.query.*;
import simpledb.parse.*;
import simpledb.plan.*;
import simpledb.index.Index;

/**
 * A modification of the basic update planner.
 * It dispatches each update statement to the corresponding
 * index planner.
 *
 * @author Edward Sciore
 */
public class IndexUpdatePlanner implements UpdatePlanner {
    private MetadataMgr mdm;

    public IndexUpdatePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public int executeInsert(InsertData data, Transaction tx) {
        String tblname = data.tableName();
        Plan p = new TablePlan(tx, tblname, mdm);

        // first, insert the record
        UpdateScan s = (UpdateScan) p.open();
        s.insert();
        RID rid = s.getRid();

        // then modify each field, inserting an index record if appropriate
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
        Iterator<Constant> valIter = data.vals().iterator();
        for (String fldname : data.fields()) {
            Constant val = valIter.next();
            s.setVal(fldname, val);

            IndexInfo ii = indexes.get(fldname);
            if (ii != null) {
                Index idx = ii.open();
                idx.insert(val, rid);
                idx.close();
            }
        }
        s.close();
        return 1;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        String tblname = data.tableName();
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
        Predicate pred = data.pred();
        // find constants value of any index within the given predicate; otherwise, IndexSelectScan can not be used
        Pair pair = null;
        for (String fld : indexes.keySet()) {
            Constant val = pred.equatesWithConstant(fld);
            if (val != null) {
                pair = new Pair(fld, val);
                break;
            }
        }
        if (pair == null)
            throw new RuntimeException("can not use IndexSelectScan");

        // apply deletion
        Plan p = new TablePlan(tx, tblname, mdm);
        p = new IndexSelectPlan(p, indexes.get(pair.fieldName), pair.val);

        IndexSelectScan s = (IndexSelectScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, delete the record's RID from every index
            RID rid = s.getRID();
            for (String fldname : indexes.keySet()) {
                if (fldname.equals(pair.fieldName)) {
                    continue;
                }
                Constant val = s.getVal(fldname);
                Index idx = indexes.get(fldname).open();
                idx.delete(val, rid);
                idx.close();
            }
            // then delete the record && the index used record
            s.delete();
            count++;
        }
        s.close();
        return count;
    }

    public int executeModify(ModifyData data, Transaction tx) {
        String tblname = data.tableName();
        String fldname = data.targetField();
        IndexInfo ii = mdm.getIndexInfo(tblname, tx).get(fldname);
        if (ii == null)
            throw new RuntimeException("can not use IndexSelectScan");

        Predicate pred = data.pred();
        // find constants value of any index within the given predicate; otherwise, IndexSelectScan can not be used
        Constant val = pred.equatesWithConstant(fldname);
        Pair pair = new Pair(fldname, val);

        if (pair == null)
            throw new RuntimeException("can not use IndexSelectScan");


        Plan p = new TablePlan(tx, tblname, mdm);
        p = new IndexSelectPlan(p, ii, pair.val);

        IndexSelectScan s = (IndexSelectScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, update the record
            Constant newval = data.newValue().evaluate(s);
            Constant oldval = s.getVal(fldname);
            s.setVal(data.targetField(), newval);

            // then update the appropriate index, if it exists
            RID rid = s.getRID();
            s.deleteIdxRecord(oldval, rid);
            s.insertIdxRecord(newval, rid);
            count++;
        }
        s.close();
        return count;
    }

    public int executeCreateTable(CreateTableData data, Transaction tx) {
        mdm.createTable(data.tableName(), data.newSchema(), tx);
        return 0;
    }

    public int executeCreateView(CreateViewData data, Transaction tx) {
        mdm.createView(data.viewName(), data.viewDef(), tx);
        return 0;
    }

    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
    }

    private class Pair {
        private String fieldName;
        private Constant val;

        Pair(String fieldName, Constant val) {
            this.fieldName = fieldName;
            this.val = val;
        }

        public String getFieldName() {
            return fieldName != null ? fieldName : null;
        }

        public Constant getVal() {
            return val != null ? val : null;
        }
    }
}
