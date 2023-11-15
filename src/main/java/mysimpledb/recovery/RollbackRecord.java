package mysimpledb.recovery;

import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;
import mysimpledb.tx.Transaction;

public class RollbackRecord implements LogRecord {
    private int txNum;

    public RollbackRecord(Page p) {
        this.txNum = p.getInt(Integer.BYTES);
    }

    @Override
    public int op() {
        return ROLLBACK;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        throw new UnsupportedOperationException();
    }

    public static int writeLogRec(MyLogMgr logMgr, int txNum) {
        byte[] recBytes = new byte[2 * Integer.BYTES];
        Page p = new Page(recBytes);
        p.setInt(0, ROLLBACK);
        p.setInt(Integer.BYTES, txNum);
        return logMgr.append(recBytes);
    }
}
