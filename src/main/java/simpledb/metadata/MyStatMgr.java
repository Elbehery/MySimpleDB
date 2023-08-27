package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class MyStatMgr {

    private Layout tblStatsLayout, fldStatsLayout;

    private TableMgr tblMgr;

    public MyStatMgr(Transaction tx, TableMgr tblMgr) {
        Schema tblStatsSchema = new Schema();
        tblStatsSchema.addStringField("tblname", TableMgr.MAX_NAME);
        tblStatsSchema.addIntField("numblocks");
        tblStatsSchema.addIntField("numrecords");
        this.tblStatsLayout = new Layout(tblStatsSchema);

        Schema fldStatsSchema = new Schema();
        fldStatsSchema.addStringField("tblname", TableMgr.MAX_NAME);
        fldStatsSchema.addStringField("fldname", TableMgr.MAX_NAME);
        fldStatsSchema.addIntField("numvalues");
        this.fldStatsLayout = new Layout(fldStatsSchema);

        this.tblMgr = tblMgr;
        this.tblMgr.createTable("tblstats", tblStatsSchema, tx);
        this.tblMgr.createTable("fldstats", fldStatsSchema, tx);
        // scan the whole database and persist stats
        refreshStats(tx);
    }

    public synchronized StatInfo getStatInfo(String tableName, Transaction tx) {
        TableScan tblCatScan = new TableScan(tx, "tblstats", this.tblStatsLayout);
        int numBlock = 0;
        int numRecs = 0;

        while (tblCatScan.next()) {
            if (tblCatScan.getString("tblname").equals(tableName)) {
                numBlock = tblCatScan.getInt("numblocks");
                numRecs = tblCatScan.getInt("numrecords");
                break;
            }
        }
        return new StatInfo(numBlock, numRecs);
    }

    private synchronized void refreshStats(Transaction tx) {
        // calc stat in memory
        Map<String, StatInfo> tableStats = new HashMap<>();
        Layout tabCatLayout = this.tblMgr.getLayout("tblcat", tx);
        TableScan tabCatScan = new TableScan(tx, "tblcat", tabCatLayout);
        while (tabCatScan.next()) {
            String tableName = tabCatScan.getString("tblname");
            Layout tblLayout = this.tblMgr.getLayout(tableName, tx);
            StatInfo info = calcTablesStats(tableName, tblLayout, tx);
            tableStats.put(tableName, info);
        }
        tabCatScan.close();

        // persist table stats in catalog
        TableScan tblStatsScan = new TableScan(tx, "tblstats", this.tblStatsLayout);
        for (String tableName : tableStats.keySet()) {
            tblStatsScan.insert();
            tblStatsScan.setString("tblname", tableName);
            tblStatsScan.setInt("numblocks", tableStats.get(tableName).blocksAccessed());
            tblStatsScan.setInt("numrecords", tableStats.get(tableName).recordsOutput());
        }
        tblStatsScan.close();

        // persist field stats in catalog
        Layout fldCatLayout = this.tblMgr.getLayout("fldcat", tx);
        TableScan fldCatScan = new TableScan(tx, "fldcat", fldCatLayout);
        TableScan fldStatsScan = new TableScan(tx, "fldstats", this.fldStatsLayout);
        while (fldCatScan.next()) {
            String tblName = fldCatScan.getString("tblname");
            String fldName = fldCatScan.getString("fldname");
            // persist
            fldStatsScan.insert();
            fldStatsScan.setString("tblname", tblName);
            fldStatsScan.setString("fldname", fldName);
            fldStatsScan.setInt("numvalues", tableStats.get(tblName).distinctValues(fldName));
        }

        fldStatsScan.close();
        fldStatsScan.close();
    }

    private synchronized StatInfo calcTablesStats(String tblname, Layout layout, Transaction tx) {
        int numRecs = 0, numBlocks = 0;
        TableScan scan = new TableScan(tx, tblname, layout);
        while (scan.next()) {
            numRecs++;
        }
        numBlocks = scan.getRid().blockNumber() + 1;
        scan.close();
        return new StatInfo(numBlocks, numRecs);
    }
}
