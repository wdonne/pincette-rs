package net.pincette.rs;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.locks.LockSupport.unpark;
import static net.pincette.rs.Util.parking;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import net.pincette.function.SideEffect;
import net.pincette.util.Util.GeneralException;

/**
 * With this class a publisher can be wrapped in a blocking iterable. It buffers the received
 * elements proportional to the size of the requests towards the publisher.
 *
 * @param <T> the element type.
 * @since 1.2
 */
public class BlockingSubscriber<T> implements Subscriber<T>, Iterable<T> {
  private final Iterator<T> iterator = new Elements();
  private final Queue<T> queue = new ConcurrentLinkedQueue<>();
  private final long requestSize;
  private final Spliterator<T> spliterator = new SplitElements();
  private final Thread thread = currentThread();
  private boolean complete;
  private Subscription subscription;

  /**
   * Creates a subscriber with a request size of 100.
   *
   * @since 1.2
   */
  public BlockingSubscriber() {
    this(100);
  }

  /**
   * Creates a subscriber with a subscription that requests <code>requestSize</code> elements at the
   * time.
   *
   * @param requestSize the number of elements the subscription asks from the publisher.
   * @since 1.2
   */
  public BlockingSubscriber(final long requestSize) {
    this.requestSize = requestSize;
  }

  public static <T> BlockingSubscriber<T> blockingSubscriber() {
    return new BlockingSubscriber<>();
  }

  public static <T> BlockingSubscriber<T> blockingSubscriber(final long requestSize) {
    return new BlockingSubscriber<>(requestSize);
  }

  /**
   * Returns an immutable iterator over the published elements. The iterator will block when there
   * are no elements while the publisher isn't completed yet.
   *
   * @return The iterator.
   * @since 1.2
   */
  public Iterator<T> iterator() {
    return iterator;
  }

  private void more() {
    if (subscription != null) {
      subscription.request(requestSize);
    }
  }

  public void onComplete() {
    complete = true;
    unpark(thread);
  }

  public void onError(final Throwable throwable) {
    if (throwable == null) {
      throw new NullPointerException("Can't throw null.");
    }

    complete = true;
    unpark(thread);
    throw new GeneralException(throwable);
  }

  public void onNext(T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    queue.add(value);

    if (queue.size() >= requestSize) {
      unpark(thread);
    }
  }

  public void onSubscribe(final Subscription subscription) {
    if (subscription == null) {
      throw new NullPointerException("A subscription can't be null.");
    }

    if (this.subscription != null) {
      subscription.cancel();
    } else {
      this.subscription = subscription;
      more();
    }
  }

  /**
   * Returns an ordered immutable spliterator that will not split.
   *
   * @return The spliterator.
   * @since 1.2
   */
  @Override
  public Spliterator<T> spliterator() {
    return spliterator;
  }

  private class Elements implements Iterator<T> {
    public boolean hasNext() {
      return !queue.isEmpty()
          || (!complete && SideEffect.<Boolean>run(this::park).andThenGet(this::hasNext));
    }

    public T next() {
      if (subscription == null || queue.isEmpty()) {
        throw new NoSuchElementException();
      }

      return queue.poll();
    }

    private boolean noData() {
      return subscription == null || (!complete && queue.isEmpty());
    }

    private void park() {
      more();

      while (noData()) {
        parking(this, requestSize * 10);
      }
    }
  }

  private class SplitElements implements Spliterator<T> {
    public int characteristics() {
      return ORDERED | IMMUTABLE;
    }

    public long estimateSize() {
      return MAX_VALUE;
    }

    public boolean tryAdvance(final Consumer<? super T> action) {
      return iterator.hasNext()
          && SideEffect.<Boolean>run(() -> action.accept(iterator.next())).andThenGet(() -> true);
    }

    public Spliterator<T> trySplit() {
      return null;
    }
  }
}
