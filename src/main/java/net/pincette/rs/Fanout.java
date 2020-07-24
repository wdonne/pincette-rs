package net.pincette.rs;

import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A processor that accepts multiple subscribers to all of which it passes all events. It passes its
 * own subscription to all subscribers.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.5
 */
public class Fanout<T> implements Processor<T, T> {
  private final List<Subscriber<? super T>> subscribers = new ArrayList<>();
  private Subscription subscription;

  public void onComplete() {
    subscribers.forEach(Subscriber::onComplete);
  }

  public void onError(final Throwable t) {
    subscribers.forEach(s -> s.onError(t));
  }

  public void onNext(final T t) {
    subscribers.forEach(s -> s.onNext(t));
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
    subscribers.forEach(s -> s.onSubscribe(subscription));
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    subscribers.add(subscriber);

    if (subscription != null) {
      subscriber.onSubscribe(subscription);
    }
  }
}
