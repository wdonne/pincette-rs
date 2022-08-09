package net.pincette.rs;

import static java.lang.Math.max;
import static java.lang.Math.round;
import static java.nio.ByteBuffer.allocate;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Util.divisibleBy;

import java.nio.ByteBuffer;
import java.util.concurrent.Flow.Processor;

/**
 * Base84 processors.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Base64 {
  private Base64() {}

  public static Processor<ByteBuffer, ByteBuffer> base64Decoder() {
    return pipe(map(Base64::join))
        .then(divisibleBy(4))
        .then(map(buffer -> getDecoder().decode(buffer)));
  }

  /**
   * Sets <code>oneLine</code> to <code>false</code>.
   *
   * @return The processor.
   */
  public static Processor<ByteBuffer, ByteBuffer> base64Encoder() {
    return base64Encoder(false);
  }

  /**
   * A processor that encodes a byte buffer stream to Base64. If <code>oneLine</code> is <code>false
   * </code> the resulting stream will be split in lines of maximal 76 characters, separated by
   * CRLF.
   *
   * @param oneLine whether the Base64 stream be one line or split in lines.
   * @return The processor.
   */
  public static Processor<ByteBuffer, ByteBuffer> base64Encoder(final boolean oneLine) {
    return pipe(divisibleBy(3))
        .then(map(buffer -> getEncoder().encode(buffer)))
        .then(map(buffer -> oneLine ? buffer : split(buffer)));
  }

  private static ByteBuffer join(final ByteBuffer buffer) {
    final int remaining = buffer.remaining();
    final ByteBuffer result = allocate(remaining);

    for (int i = 0; i < remaining; ++i) {
      final byte b = buffer.get(i);

      if (b != '\n' && b != '\r') {
        result.put(b);
      }
    }

    return truncate(result);
  }

  private static void putLineEnd(final ByteBuffer buffer) {
    buffer.put((byte) '\r');
    buffer.put((byte) '\n');
  }

  private static ByteBuffer split(final ByteBuffer buffer) {
    final int remaining = buffer.remaining();
    final ByteBuffer result = allocate(max(round(remaining * 1.2F), remaining + 2));

    for (int i = 0; i < remaining; ++i) {
      result.put(buffer.get(i));

      if ((i + 1) % 76 == 0) {
        putLineEnd(result);
      }
    }

    if (remaining % 76 != 0) {
      putLineEnd(result);
    }

    return truncate(result);
  }

  private static ByteBuffer truncate(final ByteBuffer buffer) {
    buffer.limit(buffer.position());
    buffer.position(0);

    return buffer;
  }
}
