package net.pincette.rs;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

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
    return new Source<>(new ArrayList<>(values));
  }

  @SafeVarargs
  public static <T> Publisher<T> of(final T... values) {
    return new Source<>(asList(values));
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("A subscriber can't be null.");
    }

    this.subscriber = subscriber;
    position = 0;
    subscriber.onSubscribe(new Cursor());
  }

  private class Cursor implements Subscription {
    public void cancel() {
      // Nothing to do.
    }

    @SuppressWarnings("java:S1994") // That would break it.
    public void request(final long n) {
      for (long i = 0; i < n && position < list.size(); ++i) {
        final T value = list.get(position);

        ++position; // Increment before the call to make it reentrant.
        subscriber.onNext(value);
      }

      if (position >= list.size() && !complete) {
        complete = true;
        subscriber.onComplete();
      }
    }
  }
}
