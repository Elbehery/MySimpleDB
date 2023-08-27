package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MySimpleDBClient {
    private TableMgr tblMgr;

    private Transaction tx;

    public MySimpleDBClient(Transaction tx) {
        this.tx = tx;
        this.tblMgr = new TableMgr(false, this.tx);
    }

    private List<String> getAllTablesNames() {
        List<String> tableNames = new LinkedList<>();
        Layout tblCatLayout = this.tblMgr.getLayout("tblcat", this.tx);
        TableScan tblScan = new TableScan(this.tx, "tblcat", tblCatLayout);

        while (tblScan.next()) {
            tableNames.add(tblScan.getString("tblname"));
        }
        tblScan.close();
        return tableNames;
    }

    public void PrintAllTablesNames() {
        List<String> tables = getAllTablesNames();
        for (String tbl : tables) {
            System.out.println("Table :" + tbl);
        }
    }

    private Map<String, List<String>> getAllFieldsName() {
        Map<String, List<String>> tablesFieldsMap = new HashMap<>();
        Layout fldCatlayout = this.tblMgr.getLayout("fldcat", this.tx);
        TableScan fldScan = new TableScan(this.tx, "fldcat", fldCatlayout);

        while (fldScan.next()) {
            String tblName = fldScan.getString("tblname");
            if (tablesFieldsMap.containsKey(tblName)) {
                tablesFieldsMap.get(tblName).add(fldScan.getString("fldname"));
            } else {
                List<String> fields = new LinkedList<>();
                fields.add(fldScan.getString("fldname"));
                tablesFieldsMap.put(tblName, fields);
            }
        }
        fldScan.close();
        return tablesFieldsMap;
    }

    public void PrintAllTablesFieldsNames() {
        Map<String, List<String>> tableFieldsMap = getAllFieldsName();
        for (String key : tableFieldsMap.keySet()) {
            for (String field : tableFieldsMap.get(key)) {
                System.out.println("T(" + key + ", " + field + ")");
            }
        }
    }

    public void PrintCreateStatement(String tableName) {
        StringBuilder createStmt = new StringBuilder();
        Layout fldCatlayout = this.tblMgr.getLayout("fldcat", this.tx);
        TableScan fldCatScan = new TableScan(this.tx, "fldcat", fldCatlayout);

        createStmt.append("create table ");
        createStmt.append(tableName);
        createStmt.append("(");

        while (fldCatScan.next()) {
            if (fldCatScan.getString("tblname").equals(tableName)) {
                createStmt.append(fldCatScan.getString("fldname"));
                createStmt.append(" ");
                int fldtype = fldCatScan.getInt("type");
                if (fldtype == 4) {
                    createStmt.append("integer, ");
                } else {
                    int length = fldCatScan.getInt("length");
                    createStmt.append("varchar(");
                    createStmt.append(length);
                    createStmt.append("),");
                }
            }
        }
        fldCatScan.close();
        createStmt.deleteCharAt(createStmt.length() - 1);
        createStmt.append(")");
        System.out.println(createStmt);
    }
}
