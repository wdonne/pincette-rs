package net.pincette.rs;

import static net.pincette.rs.Box.box;
import static net.pincette.rs.Buffer.buffer;
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
  private final Processor<T, T> buf = buffer(1);
  private boolean completed;
  private Monitor monitor;

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
          if (monitor == null || monitor.completed) {
            more();
          } else {
            monitor.more();
          }
        });
  }

  private void more() {
    dispatch(
        () -> {
          if (!completed) {
            subscription.request(1);
          } else {
            subscriber.onComplete();
          }
        });
  }

  @Override
  public void onComplete() {
    dispatch(
        () -> {
          completed = true;

          if (monitor == null || monitor.completed) {
            subscriber.onComplete();
          }
        });
  }

  @Override
  public void onNext(final Publisher<T> publisher) {
    if (publisher == null) {
      throw new NullPointerException("Can't emit null.");
    }

    monitor = new Monitor();
    publisher.subscribe(monitor);
  }

  @Override
  public void subscribe(final Subscriber<? super T> subscriber) {
    buf.subscribe(subscriber);
    super.subscribe(buf);
  }

  private class Monitor implements Subscriber<T> {
    private boolean completed;
    private long requested;
    private Subscription subscription;

    private void more() {
      dispatch(
          () -> {
            if (!completed) {
              ++requested;
              subscription.request(1);
            } else {
              Flatten.this.more();
            }
          });
    }

    @Override
    public void onComplete() {
      dispatch(
          () -> {
            completed = true;

            if (requested > 0) {
              Flatten.this.more();
            }
          });
    }

    @Override
    public void onError(final Throwable throwable) {
      Flatten.super.onError(throwable);
    }

    @Override
    public void onNext(final T value) {
      dispatch(
          () -> {
            --requested;
            subscriber.onNext(value);
          });
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      this.subscription = subscription;
      more();
    }
  }
}
