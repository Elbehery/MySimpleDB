package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.record.Layout;
import simpledb.tx.Transaction;
import simpledb.parse.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The object that executes SQL statements.
 *
 * @author Edward Sciore
 */
public class Planner {
    private QueryPlanner qplanner;
    private UpdatePlanner uplanner;

    public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
        this.qplanner = qplanner;
        this.uplanner = uplanner;
    }

    /**
     * Creates a plan for an SQL select statement, using the supplied planner.
     *
     * @param qry the SQL query string
     * @param tx  the transaction
     * @return the scan corresponding to the query plan
     */
    public Plan createQueryPlan(String qry, Transaction tx) {
        Parser parser = new Parser(qry);
        QueryData data = parser.query();
        verifyQuery(data, tx);
        return qplanner.createPlan(data, tx);
    }

    /**
     * Executes an SQL insert, delete, modify, or
     * create statement.
     * The method dispatches to the appropriate method of the
     * supplied update planner,
     * depending on what the parser returns.
     *
     * @param cmd the SQL update string
     * @param tx  the transaction
     * @return an integer denoting the number of affected records
     */
    public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = new Parser(cmd);
        Object data = parser.updateCmd();
        verifyUpdate(data, tx);
        if (data instanceof InsertData)
            return uplanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData)
            return uplanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData)
            return uplanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData)
            return uplanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateViewData)
            return uplanner.executeCreateView((CreateViewData) data, tx);
        else if (data instanceof CreateIndexData)
            return uplanner.executeCreateIndex((CreateIndexData) data, tx);
        else
            return 0;
    }

    // SimpleDB does not verify queries, although it should.
    private void verifyQuery(QueryData data, Transaction tx) {
        verifyTableNames(data.tables(), tx);
    }

    // SimpleDB does not verify updates, although it should.
    private void verifyUpdate(Object data, Transaction tx) {
        List<String> tableName = new ArrayList<>(1);
        if (data instanceof InsertData) {
            InsertData insertData = (InsertData) data;
            tableName.add(insertData.tableName());
            verifyTableNames(tableName, tx);

        } else if (data instanceof DeleteData) {
            DeleteData deleteData = (DeleteData) data;
            tableName.add(deleteData.tableName());
            verifyTableNames(tableName, tx);

        } else if (data instanceof ModifyData) {
            ModifyData modifyData = (ModifyData) data;
            tableName.add(modifyData.tableName());
            verifyTableNames(tableName, tx);

        } else if (data instanceof CreateViewData) {
            CreateViewData createViewData = (CreateViewData) data;
            verifyTableNames(createViewData.getQuerydata().tables(), tx);
        } else if (data instanceof CreateIndexData) {
            CreateIndexData createIndexData = (CreateIndexData) data;
            tableName.add(createIndexData.tableName());
            verifyTableNames(tableName, tx);
        } else
            throw new BadSyntaxException();

    }

    private void verifyTableNames(Collection<String> tables, Transaction tx) {
        BasicUpdatePlanner updatePlanner = (BasicUpdatePlanner) this.uplanner;
        MetadataMgr mdm = updatePlanner.getMdm();
        for (String tblname : tables) {
            Layout layout = mdm.getLayout(tblname, tx);
            if (layout == null) {
                throw new BadSyntaxException();
            }
        }
    }
}
