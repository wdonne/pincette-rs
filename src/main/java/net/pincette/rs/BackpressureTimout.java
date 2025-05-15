package net.pincette.rs;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Flow.Processor;
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
  private long requested;
  private Instant requestedTime = now();
  private boolean started;

  public BackpressureTimout(final Duration timeout) {
    checkTimeout(timeout);
  }

  public static <T> Processor<T, T> backpressureTimeout(final Duration timeout) {
    return new BackpressureTimout<>(timeout);
  }

  private void checkTimeout(final Duration timeout) {
    runAsyncAfter(
        () -> {
          if (!finished()) {
            if (started && requested == 0 && expired(timeout)) {
              super.onError(new GeneralException("Backpressure timed out"));
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
