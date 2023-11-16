package mysimpledb.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private final static long MAX_TIME = 10000;
    private Map<BlockId, Integer> locks = new HashMap<>();

    synchronized void sLock(BlockId blockId) {
        try {
            long startTime = System.currentTimeMillis();
            while (hasXLock(blockId) && !waitTooLong(startTime))
                wait(MAX_TIME);
            if (hasXLock(blockId))
                throw new LockAbortingException();
            locks.put(blockId, getLockVal(blockId) + 1);
        } catch (InterruptedException ex) {

        }
    }

    synchronized void xLock(BlockId blockId) {
        try {
            long startTime = System.currentTimeMillis();
            while (hasOtherLocks(blockId) && !waitTooLong(startTime))
                wait(MAX_TIME);
            if (hasOtherLocks(blockId))
                throw new LockAbortingException();
            locks.put(blockId, -1);
        } catch (InterruptedException ex) {

        }
    }

    synchronized void unLock(BlockId blockId) {
        int val = getLockVal(blockId);
        if (val > 1)
            locks.put(blockId, val - 1);
        else {
            locks.remove(blockId);
            notifyAll();
        }

    }

    private boolean hasXLock(BlockId blockId) {
        return getLockVal(blockId) < -1;
    }

    private boolean hasOtherLocks(BlockId blockId) {
        return getLockVal(blockId) > 1;
    }

    private int getLockVal(BlockId blockId) {
        Integer val = locks.get(blockId);
        return (val == null) ? 0 : val.intValue();
    }

    private boolean waitTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}
