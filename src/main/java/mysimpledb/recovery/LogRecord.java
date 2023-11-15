package mysimpledb.recovery;

import mysimpledb.file.Page;
import simpledb.tx.Transaction;

public interface LogRecord {
    static final int CHECKPOINT = 0, START = 1,
            COMMIT = 2, ROLLBACK = 3, SETINT = 4, SETSTRING = 5;

    int op();

    int txNumber();

    void undo(Transaction tx);

    static LogRecord createLogRecord(byte[] logRec) {
        Page buff = new Page(logRec);
        switch (buff.getInt(0)) {
            case CHECKPOINT:
                return new CheckpointRecord();
            default:
                return null;
        }
    }
}
