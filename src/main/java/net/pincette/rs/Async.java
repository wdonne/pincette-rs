package net.pincette.rs;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Emits the values produced by the stages in the order the stages arrive. The stream completes only
 * after the last stage has completed.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.5
 */
public class Async<T> implements AsyncProcessor<T> {
  private CompletionStage<Void> last;
  private Subscriber<? super T> subscriber;
  private Subscription subscription;

  public void onComplete() {
    if (subscriber != null) {
      if (last != null) {
        last.thenRun(subscriber::onComplete);
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
      final Supplier<CompletionStage<Void>> next = () -> stage.thenAccept(subscriber::onNext);

      last = last == null ? next.get() : last.thenComposeAsync(value -> next.get());
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
