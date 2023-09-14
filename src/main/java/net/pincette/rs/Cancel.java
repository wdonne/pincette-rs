package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Predicate;

/**
 * Cancels the upstream if the given condition is met.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.2
 */
public class Cancel<T> extends PassThrough<T> {
  private final Predicate<T> shouldCancel;
  private boolean cancelled;

  /**
   * Create a Cancel processor.
   *
   * @param shouldCancel the predicate that checks if the upstream should be cancelled.
   */
  public Cancel(Predicate<T> shouldCancel) {
    this.shouldCancel = shouldCancel;
  }

  public static <T> Processor<T, T> cancel(final Predicate<T> shouldCancel) {
    return new Cancel<>(shouldCancel);
  }

  @Override
  public void onNext(final T value) {
    dispatch(
        () -> {
          cancelled = shouldCancel.test(value);
          super.onNext(
              value); // Do this first to preserve order because cancel may produce messages.

          if (cancelled) {
            subscription.cancel();
          }
        });
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    super.onSubscribe(new WrapSubscription(subscription));
  }

  private class WrapSubscription implements Subscription {
    private final Subscription wrapped;

    private WrapSubscription(final Subscription subscription) {
      wrapped = subscription;
    }

    public void cancel() {
      dispatch(
          () -> {
            cancelled = true;
            wrapped.cancel();
          });
    }

    public void request(final long n) {
      dispatch(
          () -> {
            if (!cancelled) {
              wrapped.request(n);
            }
          });
    }
  }
}
