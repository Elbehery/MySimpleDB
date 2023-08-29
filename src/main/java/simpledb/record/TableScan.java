package simpledb.record;

import simpledb.file.BlockId;
import simpledb.query.*;
import simpledb.tx.Transaction;

import java.util.Date;

import static java.sql.Types.*;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 *
 * @author sciore
 */
public class TableScan implements UpdateScan {
    private Transaction tx;
    private Layout layout;
    private RecordPage rp;
    private String filename;
    private int currentslot;

    public TableScan(Transaction tx, String tblname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tblname + ".tbl";
        if (tx.size(filename) == 0)
            moveToNewBlock();
        else
            moveToBlock(0);
    }

    // Methods that implement Scan

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentslot = rp.nextAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock())
                return false;
            moveToBlock(rp.block().number() + 1);
            currentslot = rp.nextAfter(currentslot);
        }
        return true;
    }

    public int getInt(String fldname) {
        return rp.getInt(currentslot, fldname);
    }

    public String getString(String fldname) {
        return rp.getString(currentslot, fldname);
    }

    @Override
    public short getShort(String fldname) {
        return rp.getShort(currentslot, fldname);
    }

    @Override
    public boolean getBoolean(String fldname) {
        return rp.getBoolean(currentslot, fldname);
    }

    @Override
    public Date getDate(String fldname) {
        return rp.getDate(currentslot, fldname);
    }

    public Constant getVal(String fldname) {
        if (layout.schema().type(fldname) == INTEGER)
            return new Constant(getInt(fldname));
        else if (layout.schema().type(fldname) == SMALLINT)
            return new Constant(getShort(fldname));
        else if (layout.schema().type(fldname) == BOOLEAN)
            return new Constant(getBoolean(fldname));
        else if (layout.schema().type(fldname) == DATE)
            return new Constant(getDate(fldname));
        else
            return new Constant(getString(fldname));
    }


    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }

    public void close() {
        if (rp != null)
            tx.unpin(rp.block());
    }

    // Methods that implement UpdateScan

    public void setInt(String fldname, int val) {
        rp.setInt(currentslot, fldname, val);
    }

    public void setString(String fldname, String val) {
        rp.setString(currentslot, fldname, val);
    }

    public void setVal(String fldname, Constant val) {
        if (layout.schema().type(fldname) == INTEGER)
            setInt(fldname, val.asInt());
        if (layout.schema().type(fldname) == BOOLEAN)
            setBoolean(fldname, val.asBoolean());
        if (layout.schema().type(fldname) == SMALLINT)
            setShort(fldname, val.asShort());
        if (layout.schema().type(fldname) == DATE)
            setDate(fldname, val.asDate());
        else
            setString(fldname, val.asString());
    }


    @Override
    public void setBoolean(String fldname, Boolean val) {
        rp.setBoolean(currentslot, fldname, val);
    }

    @Override
    public void setShort(String fldname, short val) {
        rp.setShort(currentslot, fldname, val);
    }

    @Override
    public void setDate(String fldname, Date val) {
        rp.setDate(currentslot, fldname, val);
    }

    public void insert() {
        currentslot = rp.insertAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock())
                moveToNewBlock();
            else
                moveToBlock(rp.block().number() + 1);
            currentslot = rp.insertAfter(currentslot);
        }
    }

    public void delete() {
        rp.delete(currentslot);
    }

    public void moveToRid(RID rid) {
        close();
        BlockId blk = new BlockId(filename, rid.blockNumber());
        rp = new RecordPage(tx, blk, layout);
        currentslot = rid.slot();
    }

    public RID getRid() {
        return new RID(rp.block().number(), currentslot);
    }

    // Private auxiliary methods

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new RecordPage(tx, blk, layout);
        currentslot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new RecordPage(tx, blk, layout);
        rp.format();
        currentslot = -1;
    }

    private boolean atLastBlock() {
        return rp.block().number() == tx.size(filename) - 1;
    }
}
