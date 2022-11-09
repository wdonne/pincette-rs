package net.pincette.rs;

import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Serializer.dispatch;
import static net.pincette.rs.Util.trace;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * The processor emits the elements in the received publishers individually.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Flatten<T> extends ProcessorBase<Publisher<T>, T> {
  private static final Logger LOGGER = getLogger(Flatten.class.getName());
  private final Processor<T, T> buf = buffer(1);
  private final Monitor monitor = new Monitor();
  private boolean pendingRequest;

  /**
   * Returns a processor that emits the elements from the generated publisher individually.
   *
   * @param function the function that generates the publisher.
   * @param <T> the value type.
   * @return A new chain with the same object type.
   */
  public static <T, R> Processor<T, R> flatMap(final Function<T, Publisher<R>> function) {
    return box(map(function), flatten());
  }

  /**
   * Returns a processor that emits the elements in the received publishers individually.
   *
   * @param <T> the value type.
   * @return The processor.
   */
  public static <T> Processor<Publisher<T>, T> flatten() {
    return new Flatten<>();
  }

  @Override
  protected void emit(final long number) {
    // There is never a subscriber.
  }

  private void more() {
    trace(LOGGER, () -> "subscription request");

    if (subscription != null) {
      subscription.request(1);
    } else {
      pendingRequest = true;
    }
  }

  @Override
  public void onComplete() {
    trace(LOGGER, () -> "onComplete");
    monitor.complete();
  }

  @Override
  public void onNext(final Publisher<T> publisher) {
    if (publisher == null) {
      throw new NullPointerException("Can't emit null.");
    }

    trace(LOGGER, () -> "onNext");
    publisher.subscribe(monitor);
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    super.onSubscribe(subscription);

    if (pendingRequest) {
      pendingRequest = false;
      more();
    }
  }

  @Override
  public void subscribe(final Subscriber<? super T> subscriber) {
    buf.subscribe(subscriber);
    monitor.subscribe(buf);
  }

  private class Monitor implements Processor<T, T> {
    private boolean completed;
    private long requested;
    private boolean started;
    private Subscriber<? super T> subscriber;
    private Subscription subscription;

    private void complete() {
      dispatch(() -> completed = true);
    }

    private void more(final long n) {
      dispatch(
          () -> {
            if (subscription != null) {
              subscription.request(n);
            }
          });
    }

    public void onComplete() {
      dispatch(
          () -> {
            subscription = null;
            trace(LOGGER, () -> "monitor onComplete");

            if (completed) {
              subscriber.onComplete();
            } else {
              Flatten.this.more();
            }
          });
    }

    public void onError(final Throwable throwable) {
      subscriber.onError(throwable);
    }

    public void onNext(final T value) {
      dispatch(
          () -> {
            trace(LOGGER, () -> "monitor onNext " + value);
            --requested;
            subscriber.onNext(value);
          });
    }

    public void onSubscribe(final Subscription subscription) {
      dispatch(
          () -> {
            this.subscription = subscription;

            if (requested > 0) {
              trace(LOGGER, () -> "monitor subscription request at onSubscribe");
              more(requested);
            }
          });
    }

    public void subscribe(final Subscriber<? super T> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(new Backpressure());
    }

    private class Backpressure implements Subscription {
      public void cancel() {
        if (subscription != null) {
          subscription.cancel();
        }
      }

      public void request(final long n) {
        dispatch(
            () -> {
              requested += n;

              if (subscription != null) {
                trace(LOGGER, () -> "monitor subscription request");
                more(n);
              } else if (!started) {
                started = true;
                Flatten.this.more();
              }
            });
      }
    }
  }
}
