package net.pincette.rs.encoders;

import static java.nio.ByteBuffer.allocate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import net.pincette.rs.Encoder;

/**
 * An encoder that deflates a <code>ByteBuffer</code> stream.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Deflate implements Encoder<ByteBuffer, ByteBuffer> {
  private final Deflater deflater;
  private int lastCapacity;

  public Deflate() {
    this(new Deflater());
  }

  public Deflate(final Deflater deflater) {
    this.deflater = deflater;
  }

  public static Encoder<ByteBuffer, ByteBuffer> deflate() {
    return new Deflate();
  }

  public static Encoder<ByteBuffer, ByteBuffer> deflate(final Deflater deflater) {
    return new Deflate(deflater);
  }

  private void deflate(final List<ByteBuffer> result, final int capacity) {
    final ByteBuffer b = allocate(capacity);
    final int compressed = deflater.deflate(b);

    if (compressed > 0) {
      result.add(b.position(0).limit(compressed));
    }
  }

  public List<ByteBuffer> complete() {
    final List<ByteBuffer> result = new ArrayList<>();

    if (!deflater.finished()) {
      deflater.finish();

      while (!deflater.finished()) {
        deflate(result, lastCapacity);
      }
    }

    deflater.end();

    return result;
  }

  public List<ByteBuffer> encode(final ByteBuffer buffer) {
    final List<ByteBuffer> result = new ArrayList<>();

    if (!deflater.finished()) {
      deflater.setInput(buffer);
      lastCapacity = buffer.capacity();

      while (!deflater.needsInput()) {
        deflate(result, lastCapacity);
      }
    }

    return result;
  }
}
