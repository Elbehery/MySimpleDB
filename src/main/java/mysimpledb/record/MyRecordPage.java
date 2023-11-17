package mysimpledb.record;

import static java.sql.Types.INTEGER;

import mysimpledb.file.BlockID;
import mysimpledb.tx.MyTransaction;


public class MyRecordPage {
    private static final int USED = 1, EMPTY = 0;
    private MyLayout myLayout;
    private BlockID blockID;
    private MyTransaction tx;

    public MyRecordPage(MyLayout myLayout, BlockID blockID, MyTransaction tx) {
        this.myLayout = myLayout;
        this.blockID = blockID;
        this.tx = tx;
        tx.pin(blockID);
    }

    public String getString(int slot, String field) {
        return tx.getString(blockID, fieldOffset(slot, field));
    }

    public void setString(int slot, String fieldName, String newVal) {
        tx.setString(blockID, fieldOffset(slot, fieldName), newVal, true);
    }

    public int getInt(int slot, String fieldName) {
        return tx.getInt(blockID, fieldOffset(slot, fieldName));
    }

    public void setInt(int slot, String fieldName, int newVal) {
        tx.setInt(blockID, fieldOffset(slot, fieldName), newVal, true);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY, true);
    }

    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            setFlag(slot, EMPTY, false);
            MySchema schema = myLayout.getSchema();
            for (String fieldName : schema.fields()) {
                int fieldOffset = fieldOffset(slot, fieldName);
                if (schema.type(fieldName) == INTEGER) {
                    tx.setInt(blockID, fieldOffset, 0, false);
                } else {
                    tx.setString(blockID, fieldOffset, "", false);
                }
            }
            slot++;
        }
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot != -1)
            setFlag(newSlot, USED, true);
        return newSlot;
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public BlockID getBlockID() {
        return blockID;
    }

    // helpers
    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (isFlag(slot, flag)) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    private void setFlag(int slot, int flag, boolean okToLog) {
        tx.setInt(blockID, slotOffset(slot), flag, okToLog);
    }

    private int getFlag(int slot) {
        return tx.getInt(blockID, slotOffset(slot));
    }

    private boolean isFlag(int slot, int flag) {
        return getFlag(slot) == flag;
    }

    private boolean isValidSlot(int slot) {
        return slotOffset(slot + 1) <= tx.blockSize();
    }

    private int fieldOffset(int slot, String fieldName) {
        return slotOffset(slot) + myLayout.offset(fieldName);
    }

    private int slotOffset(int slot) {
        return slot * tx.blockSize();
    }
}
