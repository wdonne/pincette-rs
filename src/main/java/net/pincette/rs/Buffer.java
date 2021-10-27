package net.pincette.rs;

import java.util.Deque;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size.
 *
 * @param <T> the value type.
 * @since 1.7
 * @author Werner Donn\u00e8
 */
public class Buffer<T> extends Buffered<T, T> {
  /**
   * Create a buffer of <code>size</code>.
   *
   * @param size the buffer size, which must be larger than zero.
   */
  public Buffer(final int size) {
    super(size, Deque::removeLast);
  }
}
