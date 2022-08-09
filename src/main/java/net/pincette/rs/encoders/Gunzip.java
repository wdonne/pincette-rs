package net.pincette.rs.encoders;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Collections.emptyList;
import static java.util.zip.Deflater.DEFLATED;
import static net.pincette.rs.encoders.Inflate.inflate;
import static net.pincette.rs.encoders.Util.updateCrc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Inflater;
import net.pincette.rs.Encoder;
import net.pincette.util.Util.GeneralException;

/**
 * An encoder that uncompresses a <code>ByteBuffer</code> stream in GZIP format.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Gunzip implements Encoder<ByteBuffer, ByteBuffer> {
  private static final int FHCRC = 2;
  private static final int FEXTRA = 4;
  private static final int FNAME = 8;
  private static final int FCOMMENT = 16;
  private static final short GZIP_MAGIC = (short) 35615;

  private final CRC32 crc = new CRC32();
  private final Inflater inflater = new Inflater(true);
  private final Encoder<ByteBuffer, ByteBuffer> delegate = inflate(inflater);
  private boolean first = true;
  private int totalUncompressed = 0;

  public Gunzip() {
    crc.reset();
  }

  private static void discardToZero(final ByteBuffer buffer) {
    while (buffer.get() != 0)
      ;
  }

  public static Encoder<ByteBuffer, ByteBuffer> gunzip() {
    return new Gunzip();
  }

  private static void readHeader(final ByteBuffer buffer) {
    final ByteOrder originalByteOrder = buffer.order();

    buffer.order(LITTLE_ENDIAN);

    if (buffer.getShort() != GZIP_MAGIC) {
      throw new GeneralException("Not a GZIP magic number.");
    }

    if (buffer.get() != DEFLATED) {
      throw new GeneralException("Unsupported compression method (only 8 is supported).");
    }

    final byte flags = buffer.get();

    buffer.position(buffer.position() + 6); // Skip MTIME, XLF and OS.

    if ((flags & FEXTRA) == FEXTRA) {
      final short length = buffer.getShort();

      buffer.position(buffer.position() + length);
    }

    if ((flags & FNAME) == FNAME) {
      discardToZero(buffer);
    }

    if ((flags & FCOMMENT) == FCOMMENT) {
      discardToZero(buffer);
    }

    if ((flags & FHCRC) == FHCRC) {
      buffer.getShort(); // Skip CRC16.
    }

    buffer.order(originalByteOrder);
  }

  private void checkTrailer(final ByteBuffer buffer) {
    final ByteOrder originalByteOrder = buffer.order();

    buffer.order(LITTLE_ENDIAN);

    if (buffer.getInt() != (int) crc.getValue()) {
      throw new GeneralException("Incorrect GZIP CRC.");
    }

    if (buffer.getInt() != totalUncompressed) {
      throw new GeneralException("Different amount of uncompressed bytes.");
    }

    buffer.order(originalByteOrder);
  }

  public List<ByteBuffer> complete() {
    return emptyList();
  }

  public List<ByteBuffer> encode(final ByteBuffer buffer) {
    if (first) {
      first = false;
      readHeader(buffer);
    }

    final List<ByteBuffer> uncompressed = delegate.encode(buffer);

    uncompressed.forEach(
        b -> {
          updateCrc(b, crc);
          totalUncompressed += b.remaining();
        });

    if (inflater.finished() && buffer.remaining() >= 8) {
      checkTrailer(buffer);
    }

    return uncompressed;
  }
}
