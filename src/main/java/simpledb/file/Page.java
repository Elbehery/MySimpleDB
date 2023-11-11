package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.*;

public class Page {
    private ByteBuffer bb;
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    private int blockSize;

    // For creating data buffers
    public Page(int blocksize) {
        this.blockSize = blocksize;
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    // For creating log pages
    public Page(byte[] b) {
        bb = ByteBuffer.wrap(b);
    }

    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    public void setInt(int offset, int n) {
        if (offset + Integer.BYTES > blockSize) {
            throw new RuntimeException();
        }
        bb.putInt(offset, n);
    }

    public byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        if (offset + b.length + Integer.BYTES > blockSize) {
            throw new RuntimeException();
        }
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        if (offset + b.length + Integer.BYTES > blockSize) {
            throw new RuntimeException();
        }
        setBytes(offset, b);
    }

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    // a package private method, needed by FileMgr
    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }
}
