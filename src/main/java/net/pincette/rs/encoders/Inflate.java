package net.pincette.rs.encoders;

import static java.nio.ByteBuffer.allocate;
import static java.util.Collections.emptyList;
import static net.pincette.util.Util.tryToGetRethrow;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Inflater;
import net.pincette.rs.Encoder;

/**
 * An encoder that inflates a <code>ByteBuffer</code> stream.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Inflate implements Encoder<ByteBuffer, ByteBuffer> {
  private final Inflater inflater;

  public Inflate() {
    this(new Inflater());
  }

  public Inflate(final Inflater inflater) {
    this.inflater = inflater;
  }

  public static Encoder<ByteBuffer, ByteBuffer> inflate() {
    return new Inflate();
  }

  public static Encoder<ByteBuffer, ByteBuffer> inflate(final Inflater inflater) {
    return new Inflate(inflater);
  }

  private void inflate(final List<ByteBuffer> result, final int capacity) {
    final ByteBuffer b = allocate(capacity);
    final int uncompressed = tryToGetRethrow(() -> inflater.inflate(b)).orElse(0);

    if (uncompressed > 0) {
      result.add(b.position(0).limit(uncompressed));
    }
  }

  public List<ByteBuffer> complete() {
    inflater.end();

    return emptyList();
  }

  public List<ByteBuffer> encode(final ByteBuffer buffer) {
    final List<ByteBuffer> result = new ArrayList<>();

    if (!inflater.finished()) {
      inflater.setInput(buffer);

      while (!inflater.needsInput() && !inflater.finished()) {
        inflate(result, buffer.capacity() * 10);
      }
    }

    return result;
  }
}
