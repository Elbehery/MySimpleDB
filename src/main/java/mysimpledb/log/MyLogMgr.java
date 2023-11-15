package mysimpledb.log;

import mysimpledb.file.BlockID;
import mysimpledb.file.FileMgr;
import mysimpledb.file.Page;

import java.util.Iterator;

public class MyLogMgr {
    private String logFileName;
    private FileMgr fileMgr;
    private Page logPage;
    private BlockID currentBlk;
    private int lastLSN, lastSavedLSN;

    public MyLogMgr(String logFileName, FileMgr fileMgr) {
        this.logFileName = logFileName;
        this.fileMgr = fileMgr;
        byte[] data = new byte[fileMgr.getBlkSize()];
        logPage = new Page(data);
        int logFileLength = fileMgr.getFileLengthInBlocks(logFileName);
        if (logFileLength == 0) {
            currentBlk = appendNewBlock();
        } else {
            currentBlk = new BlockID(logFileName, logFileLength - 1);
            fileMgr.read(currentBlk, logPage);
        }
    }

    public void flush(int lsn) {
        if (lsn > lastSavedLSN) {
            flush();
        }
    }

    public synchronized int append(byte[] logRec) {
        int boundary = logPage.getInt(0);
        int bytesNeeded = logRec.length + Integer.BYTES;
        if (boundary - bytesNeeded < Integer.BYTES) {
            flush();
            currentBlk = appendNewBlock();
            boundary = logPage.getInt(0);
        }

        int recPos = boundary - bytesNeeded;
        logPage.setBytes(recPos, logRec);
        logPage.setInt(0, recPos);
        lastLSN += 1;
        return lastLSN;
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new MyLogIterator(fileMgr, currentBlk);
    }

    private BlockID appendNewBlock() {
        BlockID newBlkId = fileMgr.append(logFileName);
        logPage.setInt(0, fileMgr.getBlkSize());
        fileMgr.write(newBlkId, logPage);
        return newBlkId;
    }

    private void flush() {
        fileMgr.write(currentBlk, logPage);
        lastSavedLSN = lastLSN;
    }
}
