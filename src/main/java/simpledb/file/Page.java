package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.Date;

public class Page {
    private ByteBuffer bb;
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    // For creating data buffers
    public Page(int blocksize) {
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
        if (bb.capacity() - offset < Integer.BYTES) {
            throw new RuntimeException("value does not fit into Page");
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
        if (bb.capacity() - offset < b.length) {
            throw new RuntimeException("value does not fit into Page");
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
        setBytes(offset, b);
    }

    public synchronized short getShort(int offset) {
        return bb.getShort(offset);
    }

    public synchronized void setShort(int offset, short s) {
        bb.putShort(offset, s);
    }

    public synchronized boolean getBoolean(int offset) {
        byte b = bb.get(offset);
        return b == 1;
    }

    public synchronized void setBoolean(int offset, Boolean s) {
        byte b = s ? (byte) 1 : 0;
        bb.put(offset, b);
    }

    public synchronized Date getDate(int offset) {
        long l = bb.getLong(offset);
        return new Date(l);
    }

    public synchronized void setDate(int offset, Date s) {
        bb.putLong(offset, s.getTime());
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
