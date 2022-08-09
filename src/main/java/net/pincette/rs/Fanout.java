package net.pincette.rs;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static net.pincette.rs.Serializer.dispatch;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.StreamUtil.zip;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * A subscriber that accepts multiple subscribers to all of which it passes all events. The pace is
 * defined by the slowest subscriber.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.6
 */
public class Fanout<T> implements Subscriber<T> {
  private final UnaryOperator<T> duplicator;
  private final List<Backpressure> subscriptions;
  private boolean completed;
  private Subscription subscription;

  /**
   * Creates a fanout subscriber.
   *
   * @param subscribers the subscribers that all receive the values.
   * @param duplicator a function that duplicates the value for all subscribers but the first. This
   *     way there are as many copies of the value as there are subscribers. It may be <code>null
   *     </code>.
   * @since 3.0
   */
  public Fanout(final List<Subscriber<T>> subscribers, final UnaryOperator<T> duplicator) {
    this.subscriptions = subscribers.stream().map(Backpressure::new).collect(toList());
    this.duplicator = duplicator;
  }

  /**
   * Creates a fanout subscriber.
   *
   * @param subscribers the subscribers that all receive the values.
   * @param <T> the value type.
   * @return The new subscriber.
   */
  public static <T> Subscriber<T> of(final List<Subscriber<T>> subscribers) {
    return new Fanout<>(subscribers, null);
  }

  /**
   * Creates a fanout subscriber.
   *
   * @param subscribers the subscribers that all receive the values.
   * @param duplicator a function that duplicates the value for all subscribers but the first. This
   *     way there are as many copies of the value as there are subscribers. It may be <code>null
   *     </code>.
   * @param <T> the value type.
   * @return The new subscriber.
   * @since 3.0
   */
  public static <T> Subscriber<T> of(
      final List<Subscriber<T>> subscribers, final UnaryOperator<T> duplicator) {
    return new Fanout<>(subscribers, duplicator);
  }

  /**
   * Creates a fanout subscriber.
   *
   * @param subscribers the subscribers that all receive the values.
   * @param <T> the value type.
   * @return The new subscriber.
   */
  @SafeVarargs
  public static <T> Subscriber<T> of(final Subscriber<T>... subscribers) {
    return new Fanout<>(asList(subscribers), null);
  }

  public void onComplete() {
    completed = true;
    subscriptions.forEach(s -> s.subscriber.onComplete());
  }

  public void onError(final Throwable t) {
    subscriptions.forEach(s -> s.subscriber.onError(t));
  }

  public void onNext(final T v) {
    if (v == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (!completed) {
      zip(subscriptions.stream(), values(v))
          .forEach(pair -> pair.first.subscriber.onNext(pair.second));
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
      subscriptions.forEach(s -> s.subscriber.onSubscribe(s));
    }
  }

  private Stream<T> values(final T v) {
    final Supplier<T> get = () -> duplicator != null ? duplicator.apply(v) : v;

    return concat(
        Stream.of(v),
        subscriptions.size() > 1
            ? stream(subscriptions.listIterator(1)).map(s -> get.get())
            : empty());
  }

  private class Backpressure implements Subscription {
    private final Subscriber<T> subscriber;
    private boolean cancelled;
    private long requested;

    private Backpressure(final Subscriber<T> subscriber) {
      this.subscriber = subscriber;
    }

    private boolean allCancelled() {
      return subscriptions.stream().allMatch(s -> s.cancelled);
    }

    public void cancel() {
      if (!cancelled) {
        cancelled = true;

        if (allCancelled()) {
          subscription.cancel();
        }
      }
    }

    private Optional<Long> lowestCommon() {
      return subscriptions.stream()
          .filter(s -> !s.cancelled)
          .map(s -> s.requested)
          .min((r1, r2) -> (int) (r1 - r2))
          .filter(r -> r > 0);
    }

    public void request(final long number) {
      if (number <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      dispatch(
          () -> {
            requested += number;
            lowestCommon()
                .ifPresent(
                    r -> {
                      subscriptions.forEach(s -> s.requested -= r);
                      subscription.request(r);
                    });
          });
    }
  }
}
