package net.pincette.rs;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size.
 *
 * @param <T> the value type.
 * @since 1.7
 */
public class Buffer<T> implements Processor<T, T> {
  private final Deque<T> buf = new ConcurrentLinkedDeque<>();
  private final int size;
  private boolean available = true;
  private boolean error;
  private int requested;
  private Subscriber<? super T> subscriber;
  private Subscription subscription;

  /**
   * Create a buffer of <code>size</code>.
   *
   * @param size the buffer size, which must be larger than zero.
   */
  public Buffer(final int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Buffer size should be at least 1.");
    }

    this.size = size;
  }

  private void emit() {
    if (subscriber != null && subscription != null && !error) {
      flush();

      if (available && buf.isEmpty()) {
        available = false;
        subscription.request(size);
      }
    }
  }

  private void flush() {
    while (requested > 0 && !buf.isEmpty()) {
      --requested;
      subscriber.onNext(buf.removeLast());
    }
  }

  private void notifySubscriber() {
    subscriber.onSubscribe(new Backpressure());
  }

  public void onComplete() {
    if (subscriber != null && !error) {
      flush();
      subscriber.onComplete();
    }
  }

  public void onError(final Throwable t) {
    error = true;

    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void onNext(T t) {
    buf.addFirst(t);

    if (buf.size() == size) {
      available = true;
      emit();
    }
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;

    if (subscriber != null) {
      notifySubscriber();
    }
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
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
      if (number > 0) {
        requested += number;
        emit();
      }
    }
  }
}
