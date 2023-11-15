package mysimpledb.file;

import java.util.Objects;

public class BlockID {
    private String filename;
    private int blkNum;

    public BlockID(String filename, int blkNum) {
        this.filename = filename;
        this.blkNum = blkNum;
    }

    public String getFilename() {
        return filename;
    }

    public int getBlkNum() {
        return blkNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockID blockID = (BlockID) o;
        return blkNum == blockID.blkNum && Objects.equals(filename, blockID.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, blkNum);
    }
}
