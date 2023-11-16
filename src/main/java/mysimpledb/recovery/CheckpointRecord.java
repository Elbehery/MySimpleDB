package mysimpledb.recovery;

import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;

public class CheckpointRecord implements LogRecord {
    public CheckpointRecord() {
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    @Override
    public int txNumber() {
        return -1;
    }

    @Override
    public void undo(Transaction tx) {
        throw new UnsupportedOperationException();
    }

    public static int writeToLog(MyLogMgr logMgr) {
        byte[] recBytes = new byte[Integer.BYTES];
        Page p = new Page(recBytes);
        p.setInt(0, CHECKPOINT);
        return logMgr.append(recBytes);
    }
}
