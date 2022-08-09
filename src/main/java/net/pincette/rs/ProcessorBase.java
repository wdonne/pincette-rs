package net.pincette.rs;

import static java.util.logging.Level.SEVERE;
import static net.pincette.rs.Serializer.dispatch;
import static net.pincette.rs.Util.LOGGER;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * A base class for reactive streams processors.
 *
 * @param <T> the value type for the incoming elements.
 * @param <R> the value type for the outgoing elements.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public abstract class ProcessorBase<T, R> implements Processor<T, R> {
  protected Subscriber<? super R> subscriber;
  protected Subscription subscription;
  private boolean error;

  /** Cancels the upstream. */
  public void cancel() {
    dispatch(subscription::cancel);
  }

  /** Completes the downstream and cancels the upstream. */
  protected void complete() {
    onComplete();
    cancel();
  }

  protected abstract void emit(final long number);

  protected boolean getError() {
    return error;
  }

  protected void setError(final boolean value) {
    error = value;
  }

  private void notifySubscriber() {
    if (subscriber != null && subscription != null) {
      subscriber.onSubscribe(new Backpressure());
    }
  }

  public void onComplete() {
    if (!getError() && subscriber != null) {
      subscriber.onComplete();
    }
  }

  public void onError(final Throwable t) {
    if (t == null) {
      throw new NullPointerException("Can't throw null.");
    }

    setError(true);

    if (subscriber != null) {
      subscriber.onError(t);
    } else {
      LOGGER.log(SEVERE, t.getMessage(), t);
    }
  }

  public void onSubscribe(final Subscription subscription) {
    if (subscription == null) {
      throw new NullPointerException("A subscription can't be null.");
    }

    if (this.subscription != null) {
      cancel();
      this.subscription = subscription;
    } else {
      this.subscription = subscription;
      notifySubscriber();
    }
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("A subscriber can't be null.");
    }

    this.subscriber = subscriber;
    notifySubscriber();
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      ProcessorBase.this.cancel();
    }

    public void request(final long number) {
      if (number <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      if (!getError()) {
        emit(number);
      }
    }
  }
}
