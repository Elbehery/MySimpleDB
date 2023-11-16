package mysimpledb.recovery;


import mysimpledb.buffer.MyBuffer;
import mysimpledb.buffer.MyBufferMgr;
import mysimpledb.log.MyLogMgr;
import mysimpledb.tx.Transaction;
import simpledb.file.BlockId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MyRecoverMgr {
    private MyLogMgr logMgr;
    private MyBufferMgr bufferMgr;
    private Transaction tx;
    private int txNum;

    public MyRecoverMgr(MyLogMgr logMgr, MyBufferMgr bufferMgr, Transaction tx, int txNum) {
        this.logMgr = logMgr;
        this.bufferMgr = bufferMgr;
        this.tx = tx;
        this.txNum = txNum;
        StartRecord.writeToLog(logMgr, txNum);
    }

    public void commit() {
        bufferMgr.flushAll(txNum);
        int lsn = CommitRecord.writeLogRec(logMgr, txNum);
        logMgr.flush(lsn);
    }

    public int setInt(MyBuffer buffer, int offset, int newVal) {
        int oldVal = buffer.getContents().getInt(offset);
        return SetIntRecord.writeToLog(logMgr, txNum, new BlockId("", 0), offset, oldVal);
    }

    public int setString(MyBuffer buffer, int offset, String newVal) {
        String oldVal = buffer.getContents().getString(offset);
        return SetStringRecord.writeToLog(logMgr, txNum, new BlockId("", 0), offset, oldVal);
    }

    public void rollBack() {
        doRollBack();
        bufferMgr.flushAll(txNum);
        int lsn = RollbackRecord.writeLogRec(logMgr, txNum);
        logMgr.flush(lsn);
    }

    public void recover() {
        doRecover();
        bufferMgr.flushAll(txNum);
        int lsn = CheckpointRecord.writeToLog(logMgr);
        logMgr.flush(lsn);
    }

    private void doRollBack() {
        Iterator<byte[]> iterator = logMgr.iterator();
        while (iterator.hasNext()) {
            byte[] rec = iterator.next();
            LogRecord logRecord = LogRecord.createLogRecord(rec);
            if (logRecord.txNumber() == txNum) {
                if (logRecord.op() == LogRecord.START)
                    return;
                logRecord.undo(tx);
            }
        }
    }

    private void doRecover() {
        Set<Integer> finishedTxs = new HashSet<>();
        Iterator<byte[]> iterator = logMgr.iterator();
        while (iterator.hasNext()) {
            byte[] rec = iterator.next();
            LogRecord logRecord = LogRecord.createLogRecord(rec);
            if (logRecord.op() == LogRecord.CHECKPOINT)
                return;

            if (logRecord.op() == LogRecord.COMMIT || logRecord.op() == LogRecord.ROLLBACK)
                finishedTxs.add(logRecord.txNumber());
            else if (!finishedTxs.contains(logRecord.txNumber())) {
                logRecord.undo(tx);
            }
        }
    }
}
