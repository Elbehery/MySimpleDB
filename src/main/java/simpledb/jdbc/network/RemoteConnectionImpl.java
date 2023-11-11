package simpledb.jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import simpledb.file.FileMgr;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * The RMI server-side implementation of RemoteConnection.
 *
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
    private SimpleDB db;
    private Transaction currentTx;
    private Planner planner;

    private FileMgr fileMgr;

    private int currentReadBlks, currentWrittenBlks;

    /**
     * Creates a remote connection
     * and begins a new transaction for it.
     *
     * @throws RemoteException
     */
    RemoteConnectionImpl(SimpleDB db) throws RemoteException {
        this.db = db;
        currentTx = db.newTx();
        planner = db.planner();
        this.fileMgr = db.fileMgr();
        this.currentReadBlks = fileMgr.getReadBlks();
        this.currentWrittenBlks = fileMgr.getWrittenBlks();
    }

    /**
     * Creates a new RemoteStatement for this connection.
     *
     * @see simpledb.jdbc.network.RemoteConnection#createStatement()
     */
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, planner);
    }

    /**
     * Closes the connection.
     * The current transaction is committed.
     *
     * @see simpledb.jdbc.network.RemoteConnection#close()
     */
    public void close() throws RemoteException {
        currentTx.commit();
    }

// The following methods are used by the server-side classes.

    /**
     * Returns the transaction currently associated with
     * this connection.
     *
     * @return the transaction associated with this connection
     */
    Transaction getTransaction() {
        return currentTx;
    }

    /**
     * Commits the current transaction,
     * and begins a new one.
     */
    void commit() {
        currentTx.commit();
        currentTx = db.newTx();
        System.out.println(String.format("this transaction has read %d of blocks", fileMgr.getReadBlks() - this.currentReadBlks));
        System.out.println(String.format("this transaction has written %d of blocks", fileMgr.getWrittenBlks() - this.currentWrittenBlks));
    }

    /**
     * Rolls back the current transaction,
     * and begins a new one.
     */
    void rollback() {
        currentTx.rollback();
        currentTx = db.newTx();
        System.out.println(String.format("this transaction has read %d of blocks", fileMgr.getReadBlks() - this.currentReadBlks));
        System.out.println(String.format("this transaction has written %d of blocks", fileMgr.getWrittenBlks() - this.currentWrittenBlks));
    }
}

