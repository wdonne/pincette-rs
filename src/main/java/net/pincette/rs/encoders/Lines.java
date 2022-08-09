package net.pincette.rs.encoders;

import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import net.pincette.rs.Encoder;

/**
 * An encoder that receives buffers and interprets the contents as a UTF-8 encoded string. It emits
 * the individual lines in the string without the line separators.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Lines implements Encoder<ByteBuffer, String> {
  private final List<byte[]> buffers = new ArrayList<>();
  private final List<String> pendingLines = new ArrayList<>();

  public static Encoder<ByteBuffer, String> lines() {
    return new Lines();
  }

  public List<String> complete() {
    addLastLine();

    return nextLines().orElseGet(Collections::emptyList);
  }

  private void addLastLine() {
    if (!buffers.isEmpty()) {
      addLine(new byte[0], 0, 0);
    }
  }

  private void addLine(final byte[] buffer, final int start, final int end) {
    final int e = end > 0 && buffer[end - 1] == '\r' ? (end - 1) : end;
    final int l = lengthOfBuffers();
    final byte[] line = new byte[l + e - start];

    consumeBuffers(line);
    arraycopy(buffer, start, line, l, e - start);
    pendingLines.add(new String(line, UTF_8));
  }

  private void consumeBuffers(final byte[] buffer) {
    int position = 0;

    for (final byte[] b : buffers) {
      arraycopy(b, 0, buffer, position, b.length);
      position += b.length;
    }

    buffers.clear();
  }

  private void consumeLines(final byte[] buffer) {
    int start = 0;

    for (int i = 0; i < buffer.length; ++i) {
      if (buffer[i] == '\n') {
        addLine(buffer, start, i);
        start = i + 1;
      }
    }

    if (start < buffer.length) {
      final byte[] remaining = new byte[buffer.length - start];

      arraycopy(buffer, start, remaining, 0, remaining.length);
      buffers.add(remaining);
    }
  }

  public List<String> encode(final ByteBuffer buffer) {
    if (buffer.hasRemaining()) {
      final byte[] b = new byte[buffer.remaining()];

      buffer.get(b);
      consumeLines(b);
    }

    return nextLines().orElseGet(Collections::emptyList);
  }

  private int lengthOfBuffers() {
    return buffers.stream().mapToInt(b -> b.length).sum();
  }

  private Optional<List<String>> nextLines() {
    return Optional.of(pendingLines)
        .filter(lines -> !lines.isEmpty())
        .map(
            lines -> {
              final List<String> result = new ArrayList<>(pendingLines);

              pendingLines.clear();

              return result;
            });
  }
}
