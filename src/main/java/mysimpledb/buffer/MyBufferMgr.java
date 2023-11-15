package mysimpledb.buffer;

import mysimpledb.file.BlockID;
import mysimpledb.file.FileMgr;
import mysimpledb.log.MyLogMgr;
import simpledb.buffer.BufferAbortException;

public class MyBufferMgr {
    private MyBuffer[] bufferPool;
    private int numAvailable;
    private final static long MAX_TIME = 10000;

    public MyBufferMgr(FileMgr fm, MyLogMgr lm, int size) {
        this.numAvailable = size;
        bufferPool = new MyBuffer[numAvailable];
        for (int i = 0; i < numAvailable; i++) {
            bufferPool[i] = new MyBuffer(fm, lm);
        }
    }

    public synchronized int getNumAvailable() {
        return numAvailable;
    }

    public synchronized void flushAll(int txNum) {
        for (MyBuffer buffer : bufferPool) {
            if (buffer.modifyingTx() == txNum) {
                buffer.flush();
            }
        }
    }

    public synchronized void unPin(MyBuffer buffer) {
        buffer.unPin();
        if (!buffer.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    public synchronized MyBuffer pin(BlockID blk) {
        try {
            long startTime = System.currentTimeMillis();
            MyBuffer buffer = tryToPin(blk);
            while (buffer == null && !waitTooLong(startTime)) {
                wait(MAX_TIME);
                buffer = tryToPin(blk);
            }
            if (buffer == null) {
                throw new BufferAbortException();
            }
            return buffer;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private MyBuffer tryToPin(BlockID blk) {
        MyBuffer buffer = findExistingBuffer(blk);
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer();
            if (buffer == null)
                return null;
            buffer.assignToBlock(blk);
        }
        if (!buffer.isPinned())
            numAvailable--;
        buffer.pin();
        return buffer;
    }

    private MyBuffer findExistingBuffer(BlockID blk) {
        for (MyBuffer buffer : bufferPool) {
            if (buffer.getBlockID().equals(blk)) {
                return buffer;
            }
        }
        return null;
    }

    private MyBuffer chooseUnpinnedBuffer() {
        for (MyBuffer buffer : bufferPool) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }

    private boolean waitTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}
