package mysimpledb.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class MyConcurrencyMgr {
    private final static String X_LOCK = "X";
    private final static String S_LOCK = "S";
    private static LockTable lockTable = new LockTable();
    private Map<BlockId, String> locks = new HashMap<>();

    public void sLock(BlockId blockId) {
        if (locks.get(blockId) == null) {
            lockTable.sLock(blockId);
            locks.put(blockId, S_LOCK);
        }
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId);
            lockTable.xLock(blockId);
            locks.put(blockId, X_LOCK);
        }
    }

    public void release() {
        for (BlockId blockId : locks.keySet()) {
            lockTable.unLock(blockId);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId blockId) {
        return locks.get(blockId) != null && locks.get(blockId).equals(X_LOCK);
    }
}
