package mysimpledb.recovery;

import mysimpledb.file.BlockID;
import mysimpledb.file.FileMgr;
import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;
import simpledb.file.BlockId;
import simpledb.tx.Transaction;

public class SetIntRecord implements LogRecord {
    private int txNum, offset, val;
    private BlockId blockID;

    public SetIntRecord(Page p) {
        int txPos = Integer.BYTES;
        txNum = p.getInt(txPos);
        int filePos = txPos + Integer.BYTES;
        String fileName = p.getString(filePos);
        int blkPos = filePos + Page.maxLength(fileName.length());
        int blkNum = p.getInt(blkPos);
        blockID = new BlockId(fileName, blkNum);
        int offsetPos = blkPos + Integer.BYTES;
        offset = p.getInt(offsetPos);
        int valPos = offsetPos + Integer.BYTES;
        val = p.getInt(valPos);
    }

    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blockID);
        tx.setInt(blockID, offset, val, false);
        tx.unpin(blockID);
    }

    public static int writeToLog(MyLogMgr logMgr, int txnum, BlockId blk, int offset, int val) {
        int txPos = Integer.BYTES;
        int fileNamePos = txPos + Integer.BYTES;
        int blkNumPos = fileNamePos + Page.maxLength(blk.fileName().length());
        int offsetPos = blkNumPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;

        byte[] recBytes = new byte[valPos + Integer.BYTES];
        Page p = new Page(recBytes);
        p.setInt(0, SETINT);
        p.setInt(txPos, txnum);
        p.setString(fileNamePos, blk.fileName());
        p.setInt(blkNumPos, blk.number());
        p.setInt(offsetPos, offset);
        p.setInt(valPos, val);
        return logMgr.append(recBytes);
    }
}
