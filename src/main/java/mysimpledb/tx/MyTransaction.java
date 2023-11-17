package mysimpledb.tx;

import mysimpledb.buffer.MyBuffer;
import mysimpledb.buffer.MyBufferMgr;
import mysimpledb.concurrency.MyConcurrencyMgr;
import mysimpledb.file.BlockID;
import mysimpledb.file.FileMgr;
import mysimpledb.log.MyLogMgr;
import mysimpledb.recovery.MyRecoverMgr;

public class MyTransaction {
    private final static int END_OF_FILE = -1;
    private static int nextTxNum = 0;
    private MyConcurrencyMgr concurrencyMgr;
    private MyRecoverMgr recoverMgr;
    private MyBufferMgr bufferMgr;
    private FileMgr fileMgr;
    private MyBufferList bufferList;
    private int txNum;

    public MyTransaction(MyBufferMgr bufferMgr, FileMgr fileMgr, MyLogMgr logMgr) {
        this.bufferMgr = bufferMgr;
        this.fileMgr = fileMgr;
        this.txNum = nextTxNum();
        this.recoverMgr = new MyRecoverMgr(logMgr, bufferMgr, this, txNum);
        this.concurrencyMgr = new MyConcurrencyMgr();
        this.bufferList = new MyBufferList(bufferMgr);
    }

    public void commit() {
        recoverMgr.commit();
        concurrencyMgr.release();
        bufferList.unpinAll();
    }

    public void rollback() {
        recoverMgr.rollBack();
        concurrencyMgr.release();
        bufferList.unpinAll();
    }

    public void recover() {
        bufferMgr.flushAll(txNum);
        recoverMgr.recover();
    }

    public void pin(BlockID blockID) {
        bufferList.pin(blockID);
    }

    public void unpin(BlockID blockID) {
        bufferList.unpin(blockID);
    }

    public int getInt(BlockID blockID, int offset) {
        concurrencyMgr.sLock(blockID);
        MyBuffer buffer = bufferList.getBuffer(blockID);
        return buffer.getContents().getInt(offset);
    }

    public String getString(BlockID blockID, int offset) {
        concurrencyMgr.sLock(blockID);
        MyBuffer buffer = bufferList.getBuffer(blockID);
        return buffer.getContents().getString(offset);
    }

    public void setInt(BlockID blockID, int offset, int newVal, boolean okToLog) {
        concurrencyMgr.xLock(blockID);
        MyBuffer buffer = bufferList.getBuffer(blockID);
        int lsn = -1;
        if (okToLog)
            lsn = recoverMgr.setInt(buffer, offset, newVal);
        buffer.getContents().setInt(offset, newVal);
        buffer.setModified(txNum, lsn);
    }

    public void setString(BlockID blockID, int offset, String newVal, boolean okToLog) {
        concurrencyMgr.xLock(blockID);
        MyBuffer buffer = bufferList.getBuffer(blockID);
        int lsn = -1;
        if (okToLog)
            lsn = recoverMgr.setString(buffer, offset, newVal);
        buffer.getContents().setString(offset, newVal);
        buffer.setModified(txNum, lsn);
    }

    public int size(String fileName) {
        BlockID dummy = new BlockID(fileName, END_OF_FILE);
        concurrencyMgr.sLock(dummy);
        return fileMgr.getFileLengthInBlocks(fileName);
    }

    public BlockID append(String fileName) {
        BlockID dummy = new BlockID(fileName, END_OF_FILE);
        concurrencyMgr.xLock(dummy);
        return fileMgr.append(fileName);
    }

    public int blockSize() {
        return fileMgr.getBlkSize();
    }

    private static int nextTxNum() {
        return nextTxNum++;
    }
}
