package net.pincette.rs;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscription;

/**
 * This processor asks the upstream for more elements if it hasn't received any before the timeout,
 * until the stream completes.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.0
 */
public class AskForever<T> extends ProcessorBase<T, T> {
  private final Duration timeout;
  private boolean completed;
  private Instant last = now();

  public AskForever(final Duration timeout) {
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("The timeout should be positive.");
    }

    this.timeout = timeout;
  }

  public static <T> Processor<T, T> askForever(final Duration timeout) {
    return new AskForever<>(timeout);
  }

  @Override
  protected void emit(final long number) {
    subscription.request(number);
  }

  @Override
  public void onComplete() {
    completed = true;
    super.onComplete();
  }

  @Override
  public void onNext(final T value) {
    last = now();
    subscriber.onNext(value);
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    super.onSubscribe(subscription);
    runTimeout();
  }

  private void runTimeout() {
    runAsyncAfter(
        () -> {
          if (!completed) {
            if (between(last, now()).compareTo(timeout) > 0) {
              subscription.request(1);
            }

            runTimeout();
          }
        },
        timeout);
  }
}
