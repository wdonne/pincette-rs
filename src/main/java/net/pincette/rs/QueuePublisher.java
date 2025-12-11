package net.pincette.rs;

import static java.util.logging.Logger.getLogger;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import net.pincette.util.NotifyingDeque;

/**
 * A publisher that is fed by a queue.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.10.0
 */
public class QueuePublisher<T> implements Publisher<T> {
  private static final Logger LOGGER = getLogger(QueuePublisher.class.getName());

  private final Queue<T> queue = new NotifyingDeque<>(this::added, null);
  private boolean closed;
  private boolean completed;
  private final String key = UUID.randomUUID().toString();
  private final BiConsumer<QueuePublisher<T>, Long> onDepleted;
  private long requested;
  private Subscriber<? super T> subscriber;

  public QueuePublisher() {
    this(null);
  }

  /**
   * Creates a queue publisher with a function to signal the depletion of the queue.
   *
   * @param onDepleted the function that is called when the queue of the publisher is depleted while
   *     there are pending requests.
   * @since 3.11.0
   */
  public QueuePublisher(final BiConsumer<QueuePublisher<T>, Long> onDepleted) {
    this.onDepleted = onDepleted;
  }

  /**
   * Creates a queue publisher.
   *
   * @return The publisher.
   * @param <T> the value type.
   */
  public static <T> QueuePublisher<T> queuePublisher() {
    return new QueuePublisher<>();
  }

  /**
   * Creates a queue publisher with a function to signal the depletion of the queue.
   *
   * @param onDepleted the function that is called when the queue of the publisher is depleted while
   *     there are pending requests.
   * @return The publisher.
   * @param <T> the value type.
   * @since 3.11.0
   */
  public static <T> QueuePublisher<T> queuePublisher(
      final BiConsumer<QueuePublisher<T>, Long> onDepleted) {
    return new QueuePublisher<>(onDepleted);
  }

  private void added(final Queue<T> queue) {
    if (closed) {
      throw new UnsupportedOperationException("The publisher is already closed.");
    }

    trace(() -> "dispatch emit, added notification");
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
    trace(() -> "complete");
    completed = true;
    dispatch(subscriber::onComplete);
  }

  private void consume() {
    int i;

    for (i = 0; i < requested && !queue.isEmpty(); ++i) {
      subscriber.onNext(queue.remove());
    }

    requested -= i;
  }

  private void dispatch(final Runnable action) {
    Serializer.dispatch(action::run, this::onError, key);
  }

  private void emit() {
    trace(
        () ->
            "emit, completed: "
                + completed
                + ", requested: "
                + requested
                + ", queue: "
                + queue.size());

    if (subscriber != null && !completed) {
      consume();
      onDepleted();

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

  private void onDepleted() {
    if (onDepleted != null) {
      trace(() -> "dispatch onDepleted");

      dispatch(
          () -> {
            if (requested > 0 && queue.isEmpty() && !closed) {
              trace(() -> "onDepleted, requested: " + requested);
              onDepleted.accept(this, requested);
            } else {
              trace(
                  () ->
                      "Not depleted, requested: "
                          + requested
                          + ", queue: "
                          + queue.size()
                          + ", closed: "
                          + closed);
            }
          });
    }
  }

  void onError(final Throwable t) {
    LOGGER.severe(() -> onErrorMessage(t));

    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  private String onErrorMessage(final Throwable t) {
    return this
        + ": onError: "
        + t
        + "\nqueue size: "
        + queue.size()
        + "\nclosed: "
        + closed
        + "\ncompleted: "
        + completed
        + "\nrequested: "
        + requested
        + "\n";
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("A subscriber can't be null.");
    }

    this.subscriber = subscriber;
    subscriber.onSubscribe(new Backpressure());
  }

  private void trace(final Supplier<String> message) {
    Util.trace(LOGGER, this, message);
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      trace(() -> "cancel");
      queue.clear();
      close();
    }

    public void request(final long number) {
      if (number <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      dispatch(
          () -> {
            trace(() -> "Requested: " + number);
            requested += number;
            emit();
          });
    }
  }
}
