package net.pincette.rs;

import java.util.concurrent.CompletionStage;
import net.pincette.function.SideEffect;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Emits the values produced by the stages in the order the stages arrive. The stream completes
 * only after the last stage has completed.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.5
 */
public class Async<T> implements AsyncProcessor<T> {
  private CompletionStage<T> last;
  private Subscriber<? super T> subscriber;
  private Subscription subscription;

  public void onComplete() {
    if (subscriber != null) {
      if (last != null) {
        last.thenAccept(
            value -> {
              subscriber.onNext(value);
              subscriber.onComplete();
            });
      } else {
        subscriber.onComplete();
      }
    }
  }

  public void onError(final Throwable t) {
    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void onNext(final CompletionStage<T> stage) {
    if (subscriber != null) {
      last =
          last == null
              ? stage
              : last.thenComposeAsync(
                  value ->
                      SideEffect.<CompletionStage<T>>run(() -> subscriber.onNext(value))
                          .andThenGet(() -> stage));

      if (subscription != null) {
        subscription.request(1);
      }
    }
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;

    if (subscriber != null) {
      subscriber.onSubscribe(subscription);
    }
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;

    if (subscription != null) {
      subscriber.onSubscribe(subscription);
    }
  }
}
