package mysimpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer byteBuffer;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    public Page(int blkSize) {
        this.byteBuffer = ByteBuffer.allocateDirect(blkSize);
    }

    public Page(byte[] bytes) {
        this.byteBuffer = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int val) {
        byteBuffer.putInt(offset, val);
    }

    public byte[] getBytes(int offset) {
        byteBuffer.position(offset);
        int length = byteBuffer.getInt();
        byte[] buff = new byte[length];
        byteBuffer.get(buff);
        return buff;
    }

    public void setBytes(int offset, byte[] data) {
        byteBuffer.position(offset);
        byteBuffer.putInt(data.length);
        byteBuffer.put(data);
    }

    public String getString(int offset) {
        byte[] data = getBytes(offset);
        return new String(data, CHARSET);
    }

    public void setString(int offset, String str) {
        byte[] data = str.getBytes(CHARSET);
        setBytes(offset, data);
    }

    public static int maxLength(int strLength) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (int) (strLength * bytesPerChar);
    }

    ByteBuffer contents() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}
