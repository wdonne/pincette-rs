package net.pincette.rs;

import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscription;
import net.pincette.function.SideEffect;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size. It emits the buffered values as a list. This processor uses a shared
 * thread.
 *
 * @param <T> the value type.
 * @since 2.0
 * @author Werner Donn\u00e8
 */
public class Per<T> extends Buffered<T, List<T>> {
  private final int size;
  private final Duration timeout;
  private List<T> buf = new ArrayList<>();
  private boolean touched = true;

  /**
   * Create a buffer of <code>size</code>.
   *
   * @param size the buffer size, which must be larger than zero.
   */
  public Per(final int size) {
    this(size, null);
  }

  /**
   * Create a buffer of <code>size</code> with a timeout.
   *
   * @param size the buffer size, which must be larger than zero.
   * @param timeout the timeout after which the buffer is flushed. It should be positive.
   */
  public Per(final int size, final Duration timeout) {
    super(size);

    if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
      throw new IllegalArgumentException("The timeout should be positive.");
    }

    this.size = size;
    this.timeout = timeout;
  }

  private static <T> int totalSize(final List<List<T>> slices) {
    return slices.stream().mapToInt(List::size).sum();
  }

  public static <T> Processor<T, List<T>> per(final int size) {
    return new Per<>(size);
  }

  public static <T> Processor<T, List<T>> per(final int size, final Duration timeout) {
    return new Per<>(size, timeout);
  }

  private Optional<List<List<T>>> consumeBuffer(final boolean flush) {
    return Optional.of(getSlices(flush))
        .filter(s -> !s.isEmpty())
        .map(
            s ->
                SideEffect.<List<List<T>>>run(() -> buf = buf.subList(totalSize(s), buf.size()))
                    .andThenGet(() -> s));
  }

  private List<List<T>> getSlices(final boolean flush) {
    final List<T> copy = new ArrayList<>(buf);
    final int slices = copy.size() / size;
    final List<List<T>> result = new ArrayList<>(slices + 1);

    for (int i = 0; i < slices; ++i) {
      result.add(copy.subList(i * size, (i + 1) * size));
    }

    if (flush && copy.size() % size > 0) {
      result.add(copy.subList(slices * size, copy.size()));
    }

    return result;
  }

  @Override
  protected void last() {
    consumeBuffer(true).ifPresent(this::addValues);
  }

  public boolean onNextAction(final T value) {
    touched = true;
    buf.add(value);
    sendSlices(isCompleted());

    return true;
  }

  private void onNextTimeout() {
    if (!getError() && buf != null && !touched) {
      dispatch(() -> sendSlices(true));
    }

    touched = false;
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    super.onSubscribe(subscription);

    if (timeout != null) {
      runTimeout();
    }
  }

  private void runTimeout() {
    runAsyncAfter(
        () -> {
          if (!isCompleted()) {
            runTimeout();
            onNextTimeout();
          }
        },
        timeout);
  }

  private void sendSlices(final boolean flush) {
    consumeBuffer(flush)
        .ifPresent(
            list -> {
              addValues(list);
              emit();
            });
  }
}
