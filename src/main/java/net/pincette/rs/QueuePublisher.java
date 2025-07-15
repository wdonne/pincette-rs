package net.pincette.rs;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import net.pincette.util.NotifyingDeque;

/**
 * A publisher that is fed by a queue.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.10.0
 */
public class QueuePublisher<T> implements Publisher<T> {
  private final Queue<T> queue = new NotifyingDeque<>(this::added, null);
  private boolean closed;
  private boolean completed;
  private final String key = UUID.randomUUID().toString();
  private long requested;
  private Subscriber<? super T> subscriber;

  /**
   * Creates a queue publisher.
   *
   * @return The publisher.
   * @param <T> the value type.
   */
  public static <T> QueuePublisher<T> queuePublisher() {
    return new QueuePublisher<>();
  }

  private void added(final Queue<T> queue) {
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

          if (queue.isEmpty()) {
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

    for (i = 0; i < requested && !queue.isEmpty(); ++i) {
      result.add(queue.remove());
    }

    requested -= i;

    return result;
  }

  private void dispatch(final Runnable action) {
    Serializer.dispatch(action::run, this::onError, key);
  }

  private void emit() {
    if (subscriber != null && !completed) {
      final List<T> elements = consume();

      if (!elements.isEmpty()) {
        dispatch(() -> elements.forEach(e -> subscriber.onNext(e)));
      }

      if (closed && queue.isEmpty()) {
        complete();
      }
    }
  }

  /**
   * Returns the queue to which the elements are fed.
   *
   * @return The queue.
   */
  public Queue<T> getQueue() {
    return queue;
  }

  public boolean isClosed() {
    return closed;
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
      queue.clear();
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
