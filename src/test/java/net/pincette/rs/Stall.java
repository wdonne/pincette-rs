package net.pincette.rs;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

class Stall<T> implements Publisher<T> {
  private Subscriber<? super T> subscriber;

  public void subscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;

    subscriber.onSubscribe(
        new Subscription() {
          public void cancel() {
            // Nothing to do.
          }

          public void request(final long n) {
            // Stay nothing.
          }
        });
  }

  void complete() {
    if (subscriber != null) {
      subscriber.onComplete();
    }
  }
}
