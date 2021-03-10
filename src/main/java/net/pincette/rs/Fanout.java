package net.pincette.rs;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A subscriber that accepts multiple subscribers to all of which it passes all events.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.6
 */
public class Fanout<T> implements Subscriber<T> {
  private final ConcurrentMap<Backpressure, Boolean> requests = new ConcurrentHashMap<>();
  private final List<Subscriber<T>> subscribers;
  private boolean complete;
  private Subscription subscription;

  private Fanout(final List<Subscriber<T>> subscribers) {
    this.subscribers = subscribers;
    subscribers.forEach(s -> s.onSubscribe(new Backpressure()));
  }

  public static <T> Subscriber<T> of(final List<Subscriber<T>> subscribers) {
    return new Fanout<>(subscribers);
  }

  @SafeVarargs
  public static <T> Subscriber<T> of(final Subscriber<T>... subscribers) {
    return new Fanout<>(asList(subscribers));
  }

  public void onComplete() {
    complete = true;
    subscribers.forEach(Subscriber::onComplete);
  }

  public void onError(final Throwable t) {
    subscribers.forEach(s -> s.onError(t));
  }

  public void onNext(final T t) {
    if (!complete) {
      subscribers.forEach(s -> s.onNext(t));
    }
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      if (subscription != null) {
        subscription.cancel();
      }
    }

    public void request(final long l) {
      if (!complete && subscription != null) {
        requests.put(this, true);

        if (requests.size() == subscribers.size()) {
          requests.clear();
          subscription.request(1);
        }
      }
    }
  }
}
