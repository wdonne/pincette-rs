package net.pincette.rs;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;
import net.pincette.util.Util.GeneralException;

/**
 * A processor which emits an error signal if no backpressure signal was received within a given
 * timeout period.
 *
 * @param <T> the object type of the values.
 * @author Werner Donné
 * @since 3.9.0
 */
public class BackpressureTimout<T> extends ProcessorBase<T, T> {
  private final Supplier<String> errorMessage;
  private long requested;
  private Instant requestedTime = now();
  private boolean started;

  /**
   * Create the processor.
   *
   * @param timeout the time after which the error signal is sent if there was no backpressure
   *     signal.
   */
  public BackpressureTimout(final Duration timeout) {
    this(timeout, null);
  }

  /**
   * Create the processor.
   *
   * @param timeout the time after which the error signal is sent if there was no backpressure
   *     signal.
   * @param errorMessage the extra error message. It can be <code>null</code>.
   */
  public BackpressureTimout(final Duration timeout, final Supplier<String> errorMessage) {
    this.errorMessage = errorMessage;
    checkTimeout(timeout);
  }

  /**
   * Create the processor.
   *
   * @param timeout the time after which the error signal is sent if there was no backpressure
   *     signal. If it is <code>null</code> or zero, no timeout will be active.
   */
  public static <T> Processor<T, T> backpressureTimeout(final Duration timeout) {
    return backpressureTimeout(timeout, null);
  }

  /**
   * Create the processor.
   *
   * @param timeout the time after which the error signal is sent if there was no backpressure
   *     signal. If it is <code>null</code> or zero, no timeout will be active.
   * @param errorMessage the extra error message. It can be <code>null</code>.
   */
  public static <T> Processor<T, T> backpressureTimeout(
      final Duration timeout, final Supplier<String> errorMessage) {
    return timeout == null || timeout.isZero()
        ? passThrough()
        : new BackpressureTimout<>(timeout, errorMessage);
  }

  private void checkTimeout(final Duration timeout) {
    runAsyncAfter(
        () -> {
          if (!finished()) {
            if (started && requested == 0 && expired(timeout)) {
              super.onError(
                  new GeneralException(
                      "Backpressure timed out"
                          + (errorMessage != null ? (": " + errorMessage.get()) : "")));
            } else {
              checkTimeout(timeout);
            }
          }
        },
        timeout);
  }

  @Override
  protected void emit(final long number) {
    started = true;
    requested += number;
    requestedTime = now();
    subscription.request(number);
  }

  private boolean expired(final Duration timeout) {
    return between(requestedTime, now()).compareTo(timeout) > 0;
  }

  private boolean finished() {
    return completed() || cancelled() || getError();
  }

  @Override
  public void onNext(final T item) {
    --requested;
    subscriber.onNext(item);
  }
}
