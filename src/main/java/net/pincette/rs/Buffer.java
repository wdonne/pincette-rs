package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.time.Duration;
import java.util.concurrent.Flow.Processor;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size. This processor uses a shared thread.
 *
 * @param <T> the value type.
 * @since 1.7
 * @author Werner Donné
 */
public class Buffer<T> extends Buffered<T, T> {
  public Buffer(final int size) {
    super(size);
  }

  public Buffer(final int size, final Duration timeout) {
    super(size, timeout);
  }

  Buffer(final int size, final Duration timeout, final boolean internalMode) {
    super(size, timeout, internalMode);
  }

  public static <T> Processor<T, T> buffer(final int size) {
    return new Buffer<>(size);
  }

  public static <T> Processor<T, T> buffer(final int size, final Duration timeout) {
    return new Buffer<>(size, timeout);
  }

  static <T> Processor<T, T> buffer(
      final int size, final Duration timeout, final boolean internalMode) {
    return new Buffer<>(size, timeout, internalMode);
  }

  protected boolean onNextAction(final T value) {
    addValues(list(value));
    emit();

    return true;
  }
}
