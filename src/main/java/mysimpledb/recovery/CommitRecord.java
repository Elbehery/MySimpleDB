package mysimpledb.recovery;

import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;
import mysimpledb.tx.Transaction;

public class CommitRecord implements LogRecord {
    private int txNum;

    public CommitRecord(Page p) {
        this.txNum = p.getInt(Integer.BYTES);
    }

    @Override
    public int op() {
        return COMMIT;
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
        p.setInt(0, COMMIT);
        p.setInt(Integer.BYTES, txNum);
        return logMgr.append(recBytes);
    }
}
