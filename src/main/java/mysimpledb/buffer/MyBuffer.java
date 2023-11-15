package mysimpledb.buffer;

import mysimpledb.file.BlockID;
import mysimpledb.file.FileMgr;
import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;

public class MyBuffer {
    private FileMgr fileMgr;
    private MyLogMgr logMgr;
    private BlockID blockID;
    private Page contents;
    private int pins;
    private int txNum = -1;
    private int lsn = -1;

    public MyBuffer(FileMgr fileMgr, MyLogMgr logMgr) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.contents = new Page(fileMgr.getBlkSize());
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public BlockID getBlockID() {
        return this.blockID;
    }

    public Page getContents() {
        return this.contents;
    }

    public int modifyingTx() {
        return txNum;
    }

    public void setModified(int txNum, int lsn) {
        this.txNum = txNum;
        if (lsn > -1) {
            this.lsn = lsn;
        }
    }

    void pin() {
        pins++;
    }

    void unPin() {
        pins--;
    }

    void flush() {
        if (txNum > -1) {
            logMgr.flush(lsn);
            fileMgr.write(blockID, contents);
            txNum = -1;
        }
    }

    void assignToBlock(BlockID newBlkId) {
        flush();
        blockID = newBlkId;
        fileMgr.read(blockID, contents);
        pins = 0;
    }
}
