package simpledb.file;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.sql.Date;

public class Page {
   private ByteBuffer bb;
   public static Charset CHARSET = StandardCharsets.US_ASCII;
   private int blockSize;

   // For creating data buffers
   public Page(int blocksize) {
      bb = ByteBuffer.allocateDirect(blocksize);
      this.blockSize = blocksize;
   }

   // For creating log pages
   public Page(byte[] b) {
      bb = ByteBuffer.wrap(b);
   }

   public int getInt(int offset) {
      return bb.getInt(offset);
   }

   public void setInt(int offset, int n) {
      if (blockSize - offset < Integer.BYTES){
        throw new BufferOverflowException();
      }
      bb.putInt(offset, n);
   }

   public int getShort(int offset) {
      return bb.getShort(offset);
   }

   public void setShort(int offset, short n){
      if (blockSize - offset < Short.BYTES){
         throw new BufferOverflowException();
      }
      bb.putShort(offset, n);
   }

   public boolean getBoolean(int offset) {
      return bb.get(offset) != 0;
    }

   public void setBoolean(int offset, boolean b){
      if (blockSize - offset < 1){
         throw new BufferOverflowException();
      }
      // cast boolean into one byte
      byte v = b ? (byte) 1 : 0;
      bb.put(offset, v);
   }

   public byte[] getBytes(int offset) {
      bb.position(offset);
      int length = bb.getInt();
      byte[] b = new byte[length];
      bb.get(b);
      return b;
   }

   public void setBytes(int offset, byte[] b) {
      if (blockSize - offset < Integer.BYTES + b.length){
        throw new BufferOverflowException();
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

   public void setData(int offset, Date d){
      String dateString = d.toString();
      setString(offset,dateString);
   }

   public Date getDate(int offset){
      String dateString = getString(offset);
      return Date.valueOf(dateString);
   }

   public static int maxLength(int strlen) {
      float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
      return Integer.BYTES + (strlen * (int)bytesPerChar);
   }

   // a package private method, needed by FileMgr
   ByteBuffer contents() {
      bb.position(0);
      return bb;
   }
}
