package net.pincette.rs;

import static net.pincette.rs.Box.box;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Serializer.dispatch;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;

/**
 * The processor emits the elements in the received publishers individually.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Flatten<T> extends ProcessorBase<Publisher<T>, T> {
  private boolean completed;
  private Monitor monitor;
  private long requested;
  private boolean requestedUpstream;

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
    dispatch(
        () -> {
          requested += number;

          if (monitor == null) {
            more();
          } else {
            monitor.more();
          }
        });
  }

  private void more() {
    if (!requestedUpstream) {
      requestedUpstream = true;
      subscription.request(1);
    }
  }

  @Override
  public void onComplete() {
    dispatch(
        () -> {
          completed = true;

          if (monitor == null) {
            subscriber.onComplete();
          }
        });
  }

  @Override
  public void onNext(final Publisher<T> publisher) {
    requestedUpstream = false;
    monitor = new Monitor();
    publisher.subscribe(monitor);
  }

  private class Monitor implements Subscriber<T> {
    private Subscription subscription;

    private void more() {
      if (requested > 0) {
        subscription.request(1);
      }
    }

    @Override
    public void onComplete() {
      dispatch(
          () -> {
            monitor = null;

            if (!completed) {
              if (requested > 0) {
                Flatten.this.more();
              }
            } else {
              subscriber.onComplete();
            }
          });
    }

    @Override
    public void onError(final Throwable throwable) {
      Flatten.super.onError(throwable);
    }

    @Override
    public void onNext(final T value) {
      --requested;
      subscriber.onNext(value);
      more();
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      this.subscription = subscription;
      more();
    }
  }
}
