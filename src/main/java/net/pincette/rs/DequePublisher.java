package net.pincette.rs;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import net.pincette.util.NotifyingDeque;

/**
 * A publisher that is fed by a deque. The elements should be added to the head of the queue.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.1
 */
public class DequePublisher<T> implements Publisher<T> {
  private final Deque<T> deque = new NotifyingDeque<>(this::added, null);
  private boolean closed;
  private boolean completed;
  private long requested;
  private Subscriber<? super T> subscriber;

  /**
   * Creates a deque publisher.
   *
   * @return The publisher.
   * @param <T> the value type.
   */
  public static <T> DequePublisher<T> dequePublisher() {
    return new DequePublisher<>();
  }

  private void added(final Deque<T> deque) {
    if (closed) {
      throw new UnsupportedOperationException("The publisher is already closed.");
    }

    dispatch(this::emit);
  }

  /**
   * After closing, it is not allowed to add more elements to the deque. As soon as the deque is
   * depleted the subscriber will be notified of the end of the stream.
   */
  public void close() {
    dispatch(
        () -> {
          closed = true;

          if (deque.isEmpty()) {
            complete();
          }
        });
  }

  private void complete() {
    completed = true;
    dispatch(subscriber::onComplete);
  }

  private List<T> consume() {
    int i;
    final List<T> result = new ArrayList<>((int) requested);

    for (i = 0; i < requested && !deque.isEmpty(); ++i) {
      result.add(deque.removeLast());
    }

    requested -= i;

    return result;
  }

  private void dispatch(final Runnable action) {
    Serializer.dispatch(action::run, this::onError);
  }

  private void emit() {
    if (subscriber != null && !completed) {
      final List<T> elements = consume();

      if (!elements.isEmpty()) {
        dispatch(() -> elements.forEach(e -> subscriber.onNext(e)));
      }

      if (closed && deque.isEmpty()) {
        complete();
      }
    }
  }

  /**
   * Returns the deque to which the elements are fed. You should only add elements to the head of
   * the deque, otherwise the publishing order will be random.
   *
   * @return The deque.
   */
  public Deque<T> getDeque() {
    return deque;
  }

  private void onError(final Throwable t) {
    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("A subscriber can't be null.");
    }

    this.subscriber = subscriber;
    subscriber.onSubscribe(new Backpressure());
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      deque.clear();
      close();
    }

    public void request(final long number) {
      if (number <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      dispatch(
          () -> {
            requested += number;
            emit();
          });
    }
  }
}
