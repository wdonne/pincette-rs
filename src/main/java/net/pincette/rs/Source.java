package net.pincette.rs;

import java.util.List;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Emits a list of values.
 *
 * @param <T> the object type of the values.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Source<T> implements Publisher<T> {
  private final List<T> list;
  private boolean complete;
  private int position;
  private Subscriber<? super T> subscriber;

  private Source(final List<T> list) {
    this.list = list;
  }

  /**
   * Creates a publisher from a list of values.
   *
   * @param values the list of values.
   * @param <T> the object type of the values.
   * @return The publisher.
   * @since 1.0
   */
  public static <T> Publisher<T> of(final List<T> values) {
    return new Source<>(values);
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;
    position = 0;
    subscriber.onSubscribe(new Cursor());
  }

  private class Cursor implements Subscription {
    public void cancel() {
      // Nothing to do.
    }

    public void request(final long n) {
      for (long i = 0; i < n && position < list.size(); ++i) {
        subscriber.onNext(list.get(position++)); // Increment before the call to make it reentrant.
      }

      if (position >= list.size() && !complete) {
        subscriber.onComplete();
        complete = true;
      }
    }
  }
}
