package mysimpledb.log;

import mysimpledb.file.BlockID;
import mysimpledb.file.FileMgr;
import mysimpledb.file.Page;

import java.util.Iterator;

public class MyLogIterator implements Iterator<byte[]> {
    private FileMgr fileMgr;
    private BlockID currentBlk;
    private Page page;
    private int boundary, currentPosition;

    public MyLogIterator(FileMgr fileMgr, BlockID blockID) {
        this.fileMgr = fileMgr;
        this.currentBlk = blockID;
        byte[] b = new byte[fileMgr.getBlkSize()];
        page = new Page(b);
        moveToBlk(currentBlk);
    }

    @Override
    public boolean hasNext() {
        return currentPosition < fileMgr.getBlkSize() || currentBlk.getBlkNum() > 0;
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileMgr.getBlkSize()) {
            moveToBlk(new BlockID(currentBlk.getFilename(), currentBlk.getBlkNum() - 1));
        }
        byte[] rec = page.getBytes(currentPosition);
        currentPosition += Integer.BYTES + rec.length;
        return rec;
    }

    private void moveToBlk(BlockID blockID) {
        fileMgr.read(blockID, page);
        boundary = page.getInt(0);
        currentPosition = boundary;
    }
}
