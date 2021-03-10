package net.pincette.rs;

import static java.util.Optional.ofNullable;
import static net.pincette.util.Util.tryToGet;

import java.util.function.Function;
import net.pincette.function.SideEffect;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Transforms a reactive stream. Null values are neither processed nor emitted.
 *
 * @param <T> the type of the incoming values.
 * @param <R> the type of the outgoing values.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Mapper<T, R> implements Processor<T, R> {
  private final Function<T, R> map;
  protected Subscription subscription;
  private boolean error;
  private Subscriber<? super R> subscriber;

  public Mapper(final Function<T, R> map) {
    this.map = map;
  }

  /**
   * With this method a subclass can regulate the backpressure.
   *
   * @param number the strictly positive number of elements.
   * @since 1.6
   */
  protected boolean canRequestMore(long number) {
    return true;
  }

  /**
   * Request one more element from the upstream.
   *
   * @since 1.4
   */
  protected void more() {
    more(1);
  }

  /**
   * Request more elements from the upstream. This will only have an effect wheb there is a
   * subscription and the stream is not in an error state.
   *
   * @param number the strictly positive number of elements.
   * @since 1.4
   */
  protected void more(final long number) {
    if (subscription != null && number > 0 && !error && canRequestMore(number)) {
      subscription.request(number);
    }
  }

  private R newValue(final T value) {
    return ofNullable(value)
        .flatMap(
            v ->
                tryToGet(
                    () -> map.apply(v),
                    e -> SideEffect.<R>run(() -> onError(e)).andThenGet(() -> null)))
        .orElse(null);
  }

  private void notifySubscriber() {
    subscriber.onSubscribe(new Backpressure());
  }

  public void onComplete() {
    if (subscriber != null && !error) {
      subscriber.onComplete();
    }
  }

  public void onError(final Throwable t) {
    setError(true);

    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void onNext(final T value) {
    if (subscriber != null) {
      final R newValue = newValue(value);

      if (newValue != null) {
        subscriber.onNext(newValue);
      } else {
        more(); // Keep the upstream alive.
      }
    }
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;

    if (subscriber != null) {
      notifySubscriber();
    }
  }

  protected void setError(final boolean value) {
    error = value;
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    this.subscriber = subscriber;

    if (subscriber != null && subscription != null) {
      notifySubscriber();
    }
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      if (subscription != null) {
        subscription.cancel();
      }
    }

    public void request(final long number) {
      more(number);
    }
  }
}
