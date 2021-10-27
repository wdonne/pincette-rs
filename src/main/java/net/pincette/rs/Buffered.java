package net.pincette.rs;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

class Buffered<T, R> implements Processor<T, R> {
  private final Deque<T> buf = new ConcurrentLinkedDeque<>();
  private final Function<Deque<T>, R> consumerBuffer;
  private final int size;
  private boolean available = true;
  private boolean error;
  private int requested;
  private Subscriber<? super R> subscriber;
  private Subscription subscription;

  Buffered(final int size, final Function<Deque<T>, R> consumerBuffer) {
    if (size < 1) {
      throw new IllegalArgumentException("Buffer size should be at least 1.");
    }

    this.size = size;
    this.consumerBuffer = consumerBuffer;
  }

  private void emit() {
    if (subscriber != null && subscription != null && !error) {
      flush();

      if (available && buf.isEmpty()) {
        available = false;
        subscription.request(size);
      }
    }
  }

  private void flush() {
    while (requested > 0 && !buf.isEmpty()) {
      --requested;
      subscriber.onNext(consumerBuffer.apply(buf));
    }
  }

  private void notifySubscriber() {
    subscriber.onSubscribe(new Backpressure());
  }

  public void onComplete() {
    if (subscriber != null && !error) {
      flush();
      subscriber.onComplete();
    }
  }

  public void onError(final Throwable t) {
    error = true;

    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void onNext(T t) {
    buf.addFirst(t);

    if (buf.size() == size) {
      available = true;
      emit();
    }
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;

    if (subscriber != null) {
      notifySubscriber();
    }
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    this.subscriber = subscriber;

    if (subscriber != null && subscription != null) {
      notifySubscriber();
    }
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      if (subscription != null) {
        subscription.cancel();
      }
    }

    public void request(final long number) {
      if (number > 0) {
        requested += number;
        emit();
      }
    }
  }
}
