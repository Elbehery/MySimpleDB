package mysimpledb.record;

import java.util.Objects;

public class MyRID {
    private int blkNum;
    private int slot;

    public MyRID(int blkNum, int slot) {
        this.blkNum = blkNum;
        this.slot = slot;
    }

    public int getBlkNum() {
        return blkNum;
    }

    public int getSlot() {
        return slot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyRID myRID = (MyRID) o;
        return blkNum == myRID.blkNum && slot == myRID.slot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(blkNum, slot);
    }
}
