package mysimpledb.recovery;

import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;
import simpledb.file.BlockId;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetStringRecord implements LogRecord {
    private int txNum, offset;
    private String val;
    private BlockId blockId;

    public SetStringRecord(Page p) {
        int txPos = Integer.BYTES;
        txNum = p.getInt(txPos);
        int filePos = txPos + Integer.BYTES;
        String fileName = p.getString(filePos);
        int blkPos = filePos + Page.maxLength(fileName.length());
        int blkNum = p.getInt(blkPos);
        blockId = new BlockId(fileName, blkNum);
        int offsetPos = blkPos + Integer.BYTES;
        offset = p.getInt(offsetPos);
        int valPos = offsetPos + Integer.BYTES;
        val = p.getString(valPos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blockId);
        tx.setString(blockId, offset, val, false);
        tx.unpin(blockId);
    }

    public static int writeToLog(MyLogMgr lm, int txnum, BlockId blk, int offset, String val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + simpledb.file.Page.maxLength(blk.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());

        byte[] recBytes = new byte[reclen];
        Page p = new Page(recBytes);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blk.fileName());
        p.setInt(bpos, blk.number());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(recBytes);
    }
}
