package net.pincette.rs;

import java.util.UUID;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * A base class for reactive streams processors.
 *
 * @param <T> the value type for the incoming elements.
 * @param <R> the value type for the outgoing elements.
 * @author Werner Donné
 * @since 3.0
 */
public abstract class ProcessorBase<T, R> implements Processor<T, R> {
  protected Subscriber<? super R> subscriber;
  protected Subscription subscription;
  private boolean cancelled;
  private boolean completed;
  private boolean error;
  private final String key = UUID.randomUUID().toString();
  private Throwable pendingException;
  private boolean subscriberNotified;

  /** Cancels the upstream. */
  public void cancel() {
    cancelling();
  }

  /**
   * Provides subclasses the opportunity to flush stuff when the stream is cancelled. The
   * implementation cancels the subscription, which subclasses should also do after doing their own
   * thing.
   */
  protected void cancelling() {
    dispatch(
        () -> {
          if (!cancelled) {
            cancelled = true;
            subscription.cancel();
          }
        });
  }

  protected boolean cancelled() {
    return cancelled;
  }

  /** Completes the downstream and cancels the upstream. */
  protected void complete() {
    onComplete();
    cancel();
  }

  protected boolean completed() {
    return completed;
  }

  protected void dispatch(final Runnable action) {
    Serializer.dispatch(action::run, this::onError, key);
  }

  protected abstract void emit(final long number);

  protected boolean getError() {
    return error;
  }

  private void notifySubscriber() {
    if (subscriber != null) {
      if (pendingException != null) {
        subscriber.onError(pendingException);
      } else if (subscription != null && !subscriberNotified) {
        subscriberNotified = true;
        subscriber.onSubscribe(new Backpressure());
      }
    }
  }

  public void onComplete() {
    if (!getError()) {
      if (subscriber != null) {
        subscriber.onComplete();
      } else {
        completed = true;
      }
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
      pendingException = t;
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

  protected void setError(final boolean value) {
    error = value;
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

      if (!getError() && !cancelled) {
        if (completed) {
          onComplete();
        } else {
          emit(number);
        }
      }
    }
  }
}
