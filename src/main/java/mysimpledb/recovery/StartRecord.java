package mysimpledb.recovery;

import mysimpledb.file.Page;
import mysimpledb.log.MyLogMgr;

public class StartRecord implements LogRecord {
    private int txNum;

    public StartRecord(Page p) {
        this.txNum = p.getInt(Integer.BYTES);
    }

    @Override
    public int op() {
        return START;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        throw new UnsupportedOperationException();
    }

    public static int writeToLog(MyLogMgr logMgr, int txNum) {
        byte[] recBytes = new byte[2 * Integer.BYTES];
        Page p = new Page(recBytes);
        p.setInt(0, START);
        p.setInt(Integer.BYTES, txNum);
        return logMgr.append(recBytes);
    }
}
