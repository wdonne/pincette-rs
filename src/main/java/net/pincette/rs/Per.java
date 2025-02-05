package net.pincette.rs;

import static java.util.Optional.ofNullable;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static net.pincette.util.StreamUtil.generate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow.Processor;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size. It emits the buffered values as a list. This processor uses a shared
 * thread.
 *
 * @param <T> the value type.
 * @since 2.0
 * @author Werner Donné
 */
public class Per<T> extends Buffered<T, List<T>> {
  private final Deque<T> buf = new LinkedList<>();
  private final int size;
  private final Duration timeout;
  private boolean timerOn;

  /**
   * Create a buffer of <code>size</code>. The <code>requestTimeout</code> is set to <code>null
   * </code>.
   *
   * @param size the buffer size, which must be larger than zero.
   */
  public Per(final int size) {
    this(size, null);
  }

  /**
   * Create a buffer of <code>size</code> with a timeout. The <code>requestTimeout</code> is set to
   * <code>null</code>.
   *
   * @param size the buffer size, which must be larger than zero.
   * @param timeout the timeout after which the buffer is flushed. It should be positive.
   */
  public Per(final int size, final Duration timeout) {
    this(size, timeout, null);
  }

  /**
   * Create a buffer of <code>size</code> with a timeout.
   *
   * @param size the buffer size, which must be larger than zero.
   * @param timeout the timeout after which the buffer is flushed. It should be positive.
   * @param requestTimeout the time after which an additional element is requested, even if the
   *     upstream publisher hasn't sent all requested elements yet. This provides the opportunity to
   *     the publisher to complete properly when it has fewer elements left than the buffer size. It
   *     may be <code>null</code>.
   * @since 3.0.2
   */
  public Per(final int size, final Duration timeout, final Duration requestTimeout) {
    super(size, requestTimeout);

    if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
      throw new IllegalArgumentException("The timeout should be positive.");
    }

    this.size = size;
    this.timeout = timeout;
  }

  public static <T> Processor<T, List<T>> per(final int size) {
    return new Per<>(size);
  }

  public static <T> Processor<T, List<T>> per(final int size, final Duration timeout) {
    return new Per<>(size, timeout);
  }

  public static <T> Processor<T, List<T>> per(
      final int size, final Duration timeout, final Duration requestTimeout) {
    return new Per<>(size, timeout, requestTimeout);
  }

  private Optional<List<List<T>>> consumeBuffer(final boolean flush) {
    return Optional.of(getSlices(flush)).filter(s -> !s.isEmpty());
  }

  private List<T> getSlice(final boolean flush) {
    return buf.size() >= size || (flush && !buf.isEmpty()) ? getSlice() : null;
  }

  private List<T> getSlice() {
    final List<T> result = new ArrayList<>(size);

    for (int i = 0; i < size && !buf.isEmpty(); ++i) {
      result.add(buf.removeLast());
    }

    return result;
  }

  private List<List<T>> getSlices(final boolean flush) {
    return generate(() -> ofNullable(getSlice(flush))).toList();
  }

  private boolean hasTimeout() {
    return timeout != null && !timeout.isZero() && !timeout.isNegative();
  }

  @Override
  protected void last() {
    consumeBuffer(true).ifPresent(this::addValues);
  }

  public boolean onNextAction(final T value) {
    buf.addFirst(value);

    if (shouldRunTimeout()) {
      runTimeout();
    }

    return sendSlices(isCompleted());
  }

  private void onNextTimeout() {
    if (!isCompleted() && !getError() && !buf.isEmpty()) {
      sendSlices(true);
    }

    timerOn = false;
  }

  private void runTimeout() {
    timerOn = true;
    runAsyncAfter(() -> dispatch(this::onNextTimeout), timeout);
  }

  private boolean sendSlices(final boolean flush) {
    return consumeBuffer(flush)
        .map(
            list -> {
              addValues(list);
              emit();

              return true;
            })
        .orElse(false);
  }

  private boolean shouldRunTimeout() {
    return hasTimeout() && !timerOn && buf.size() != size;
  }
}
