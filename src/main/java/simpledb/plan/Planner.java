package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.tx.Transaction;
import simpledb.parse.*;

import java.util.Collection;
import java.util.LinkedList;
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
        verifyUpdate(data);
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
        BasicUpdatePlanner basicUpdatePlanner = (BasicUpdatePlanner) this.uplanner;
        MetadataMgr mdm = basicUpdatePlanner.getMdm();
        List<Layout> tableLayouts = new LinkedList<>();

        for (String tblname : data.tables()) {
            Layout layout = mdm.getLayout(tblname, tx);
            tableLayouts.add(layout);
        }
        verifyPredicate(tableLayouts, data.pred());
    }

    // SimpleDB does not verify updates, although it should.
    private void verifyUpdate(Object data) {
    }

    private void verifyPredicate(List<Layout> tableLayouts, Predicate predicate) {
        List<Term> terms = predicate.getTerms();

        for (Term term : terms) {
            Expression lhs = term.getLhs();
            Expression rhs = term.getRhs();

            if (lhs.isFieldName() && rhs.isFieldName()) {
                int lhsType = getFieldType(tableLayouts, lhs.asFieldName());
                int rhsType = getFieldType(tableLayouts, rhs.asFieldName());

                if (lhsType == -1 || rhsType == -1 || lhsType != rhsType) {
                    throw new BadSyntaxException();
                }

            } else if (lhs.isFieldName() && rhs.isConstant()) {
                int lhsType = getFieldType(tableLayouts, lhs.asFieldName());
                if (lhsType == -1)
                    throw new BadSyntaxException();

                if (lhsType == 4 && !rhs.asConstant().isInt())
                    throw new BadSyntaxException();

                if (lhsType == 12 && !rhs.asConstant().isString())
                    throw new BadSyntaxException();

            } else if (lhs.isConstant() && rhs.isFieldName()) {
                int rhsType = getFieldType(tableLayouts, rhs.asFieldName());
                if (rhsType == -1)
                    throw new BadSyntaxException();

                if (rhsType == 4 && !lhs.asConstant().isInt())
                    throw new BadSyntaxException();

                if (rhsType == 12 && !lhs.asConstant().isString())
                    throw new BadSyntaxException();

            } else if (lhs.isConstant() && rhs.isConstant()) {
                if (!lhs.asConstant().equals(rhs.asConstant()))
                    throw new BadSyntaxException();
            }
        }
    }

    private int getFieldType(List<Layout> tablesLayout, String fieldName) {
        for (Layout layout : tablesLayout) {
            if (layout.schema().hasField(fieldName)) {
                return layout.schema().type(fieldName);
            }
        }
        return -1;
    }
}
