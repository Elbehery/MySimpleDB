package mysimpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private File dbDir;
    private int blkSize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles;

    public FileMgr(File dbDir, int blkSize) {
        this.dbDir = dbDir;
        this.blkSize = blkSize;
        this.openFiles = new HashMap<>();
        this.isNew = !dbDir.exists();

        // create db dir if notExist
        if (isNew) {
            dbDir.mkdirs();
        }

        // remove temp tables leftover
        for (String filename : dbDir.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDir, filename).delete();
            }
        }
    }

    public int getBlkSize() {
        return blkSize;
    }

    public boolean isNew() {
        return isNew;
    }

    public synchronized void read(BlockID blockID, Page page) {
        try {
            RandomAccessFile f = getFile(blockID.getFilename());
            f.seek(blockID.getBlkNum() * blkSize);
            f.getChannel().read(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blockID);
        }
    }

    public synchronized void write(BlockID blockID, Page page) {
        try {
            RandomAccessFile f = getFile(blockID.getFilename());
            f.seek(blockID.getBlkNum() * blkSize);
            f.getChannel().write(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block" + blockID);
        }
    }

    public synchronized BlockID append(String fileName) {
        int newBlockNumber = getFileLengthInBlocks(fileName);
        BlockID newBlk = new BlockID(fileName, newBlockNumber);
        byte[] data = new byte[blkSize];

        try {
            RandomAccessFile f = getFile(fileName);
            f.seek(newBlockNumber * blkSize);
            f.write(data);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + newBlk);
        }
        return newBlk;
    }

    public int getFileLengthInBlocks(String fileName) {
        try {
            RandomAccessFile f = getFile(fileName);
            return (int) f.length() / blkSize;
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + fileName);
        }
    }

    private RandomAccessFile getFile(String fileName) throws IOException {
        RandomAccessFile f = openFiles.get(fileName);
        if (f == null) {
            f = new RandomAccessFile(new File(dbDir, fileName), "rws");
            openFiles.put(fileName, f);
        }
        return f;
    }
}
