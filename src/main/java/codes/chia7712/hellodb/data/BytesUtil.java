package codes.chia7712.hellodb.data;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Comparator;

public final class BytesUtil {
  private static final Comparator<Cell> CELL_COMPARATOR = new CellComparator();
  private static final String UTF8_ENCODING = "UTF-8";
  private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);

  public static Comparator<Cell> getComparator() {
    return CELL_COMPARATOR;
  }

  public static byte[] toBytes(String s) {
    return s.getBytes(UTF8_CHARSET);
  }

  public static byte[] toBytes(float value) {
    return ByteBuffer.allocate(Float.BYTES)
            .putFloat(value)
            .array();
  }

  public static byte[] toBytes(short value) {
    return ByteBuffer.allocate(Short.BYTES)
            .putShort(value)
            .array();
  }

  public static byte[] toBytes(double value) {
    return ByteBuffer.allocate(Double.BYTES)
            .putDouble(value)
            .array();
  }

  public static byte[] toBytes(int value) {
    return ByteBuffer.allocate(Integer.BYTES)
            .putInt(value)
            .array();
  }

  public static byte[] toBytes(long value) {
    return ByteBuffer.allocate(Long.BYTES)
            .putLong(value)
            .array();
  }

  private BytesUtil() {
  }
}
