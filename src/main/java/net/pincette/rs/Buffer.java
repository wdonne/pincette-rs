package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.util.concurrent.Flow.Processor;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size. This processor uses a shared thread. Internally the buffer is not
 * bounded, so a publisher that doesn't respect backpressure may grow it infinitely. This processor
 * itself will always respect backpressure for its subscribers.
 *
 * @param <T> the value type.
 * @since 1.7
 * @author Werner Donn\u00e8
 */
public class Buffer<T> extends Buffered<T, T> {
  public Buffer(final int size) {
    super(size);
  }

  public static <T> Processor<T, T> buffer(final int size) {
    return new Buffer<>(size);
  }

  protected boolean onNextAction(final T value) {
    addValues(list(value));
    emit();

    return true;
  }
}
