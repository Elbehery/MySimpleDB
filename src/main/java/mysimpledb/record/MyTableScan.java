package mysimpledb.record;

import mysimpledb.file.BlockID;
import mysimpledb.tx.MyTransaction;

public class MyTableScan {
    private MyTransaction tx;
    private MyRecordPage rp;
    private MyLayout layout;
    private int currentSlot;
    private String fileName;

    public MyTableScan(MyTransaction tx, MyLayout layout, String tblName) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tblName + ".tbl";
        if (tx.size(fileName) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void close() {
        if (rp != null)
            tx.unpin(rp.getBlockID());
    }

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentSlot = rp.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock())
                return false;
            moveToBlock(rp.getBlockID().getBlkNum() + 1);
            currentSlot = rp.nextAfter(currentSlot);
        }
        return true;
    }

    public int getInt(String fieldName) {
        return rp.getInt(currentSlot, fieldName);
    }

    public String getString(String fieldName) {
        return rp.getString(currentSlot, fieldName);
    }

    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    public void setInt(String fieldName, int newVal) {
        rp.setInt(currentSlot, fieldName, newVal);
    }

    public void setString(String fieldName, String newVal) {
        rp.setString(currentSlot, fieldName, newVal);
    }

    public void insert() {
        currentSlot = rp.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock())
                moveToNewBlock();
            else
                moveToBlock(rp.getBlockID().getBlkNum() + 1);
            currentSlot = rp.insertAfter(currentSlot);
        }
    }

    public void delete() {
        rp.delete(currentSlot);
    }

    public void moveToRID(MyRID rid) {
        moveToBlock(rid.getBlkNum() + 1);
        currentSlot = rid.getSlot();
    }

    public MyRID getRID() {
        return new MyRID(rp.getBlockID().getBlkNum(), currentSlot);
    }

    // helpers
    private void moveToBlock(int blockNumber) {
        close();
        BlockID blockID = new BlockID(fileName, blockNumber);
        rp = new MyRecordPage(layout, blockID, tx);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockID blockID = tx.append(fileName);
        rp = new MyRecordPage(layout, blockID, tx);
        rp.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return rp.getBlockID().getBlkNum() == tx.size(fileName) - 1;
    }
}
