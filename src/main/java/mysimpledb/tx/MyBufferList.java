package mysimpledb.tx;

import mysimpledb.buffer.MyBuffer;
import mysimpledb.buffer.MyBufferMgr;
import mysimpledb.file.BlockID;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MyBufferList {
    private Map<BlockID, MyBuffer> buffers;
    private List<BlockID> pins;
    private MyBufferMgr bufferMgr;

    public MyBufferList(MyBufferMgr bufferMgr) {
        this.bufferMgr = bufferMgr;
        this.buffers = new HashMap<>();
        this.pins = new LinkedList<>();
    }

    void pin(BlockID blockID) {
        MyBuffer buffer = bufferMgr.pin(blockID);
        buffers.put(blockID, buffer);
        pins.add(blockID);
    }

    void unpin(BlockID blockID) {
        MyBuffer buffer = buffers.get(blockID);
        bufferMgr.unPin(buffer);
        pins.remove(blockID);
        if (!pins.contains(blockID))
            buffers.remove(blockID);
    }

    void unpinAll() {
        for (BlockID blockID : pins) {
            bufferMgr.unPin(buffers.get(blockID));
        }
        buffers.clear();
        pins.clear();
    }

    MyBuffer getBuffer(BlockID blockID) {
        return buffers.get(blockID);
    }
}
