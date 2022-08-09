package net.pincette.rs.encoders;

import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.wrap;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.DEFLATED;
import static net.pincette.rs.encoders.Deflate.deflate;
import static net.pincette.rs.encoders.Util.updateCrc;
import static net.pincette.util.Collections.concat;
import static net.pincette.util.Collections.list;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import net.pincette.rs.Encoder;

/**
 * An encoder that compresses a <code>ByteBuffer</code> stream in GZIP format.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Gzip implements Encoder<ByteBuffer, ByteBuffer> {
  private static final int GZIP_MAGIC = 0x8b1f;
  private static final byte OS_UNKNOWN = (byte) 255;
  private static final byte[] HEADER =
      new byte[] { // From java.util.zip.GZIPOutputStream.
        (byte) GZIP_MAGIC, // Magic number (short)
        (byte) (GZIP_MAGIC >> 8), // Magic number (short)
        DEFLATED, // Compression method (CM)
        0, // Flags (FLG)
        0, // Modification time MTIME (int)
        0, // Modification time MTIME (int)
        0, // Modification time MTIME (int)
        0, // Modification time MTIME (int)
        0, // Extra flags (XFLG)
        OS_UNKNOWN // Operating system (OS)
      };

  private final CRC32 crc = new CRC32();
  private final Encoder<ByteBuffer, ByteBuffer> delegate =
      deflate(new Deflater(DEFAULT_COMPRESSION, true));
  private boolean first = true;
  private int totalIn = 0;

  public Gzip() {
    crc.reset();
  }

  public static Encoder<ByteBuffer, ByteBuffer> gzip() {
    return new Gzip();
  }

  public List<ByteBuffer> complete() {
    return concat(delegate.complete(), list(trailer()));
  }

  public List<ByteBuffer> encode(final ByteBuffer buffer) {
    totalIn += updateCrc(buffer, crc).remaining();

    if (first) {
      first = false;

      return concat(list(wrap(HEADER)), delegate.encode(buffer));
    }

    return delegate.encode(buffer);
  }

  private ByteBuffer trailer() {
    return allocate(128)
        .order(LITTLE_ENDIAN)
        .putInt((int) crc.getValue())
        .putInt(totalIn)
        .position(0);
  }
}
