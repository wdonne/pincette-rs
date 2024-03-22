package net.pincette.rs.encoders;

import static java.lang.Math.min;
import static net.pincette.util.StreamUtil.generate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import net.pincette.rs.Encoder;

/**
 * Produces byte buffers the size of which is divisible by a certain number, where the last one may
 * be smaller than that number.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class DivisibleBy implements Encoder<ByteBuffer, ByteBuffer> {
  private final List<ByteBuffer> buffers = new ArrayList<>();
  private final int n;

  public DivisibleBy(final int n) {
    this.n = n;
  }

  public static Encoder<ByteBuffer, ByteBuffer> divisibleBy(final int n) {
    return new DivisibleBy(n);
  }

  public List<ByteBuffer> complete() {
    return consumeBuffers(true);
  }

  private List<ByteBuffer> consumeBuffers(final boolean completed) {
    return generate(() -> nextBuffer(completed)).toList();
  }

  private Optional<Integer> count() {
    return Optional.of(countEnough()).filter(c -> c >= n);
  }

  private int countEnough() {
    int count = 0;

    for (int i = 0; i < buffers.size() && count < n; ++i) {
      count += buffers.get(i).remaining();
    }

    return count;
  }

  public List<ByteBuffer> encode(final ByteBuffer buffer) {
    if (buffer.hasRemaining()) {
      buffers.add(buffer);
    }

    return consumeBuffers(false);
  }

  private byte[] fillBuffer(final byte[] buffer) {
    int filled = 0;

    while (filled < buffer.length) {
      final ByteBuffer b = buffers.remove(0);
      final int size = min(buffer.length - filled, b.remaining());

      b.get(buffer, filled, size);
      filled += size;

      if (b.hasRemaining()) {
        buffers.add(0, b);
      }
    }

    return buffer;
  }

  private Optional<ByteBuffer> lastBuffer(final boolean completed) {
    return Optional.of(buffers)
        .filter(b -> !b.isEmpty() && completed)
        .map(b -> countEnough())
        .map(byte[]::new)
        .map(this::fillBuffer)
        .map(ByteBuffer::wrap);
  }

  private Optional<ByteBuffer> nextBuffer(final boolean completed) {
    return count()
        .map(count -> new byte[count - count % n])
        .map(this::fillBuffer)
        .map(ByteBuffer::wrap)
        .or(() -> lastBuffer(completed));
  }
}
