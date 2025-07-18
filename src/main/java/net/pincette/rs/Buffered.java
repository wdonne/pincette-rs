package net.pincette.rs;

import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Util.throwBackpressureViolation;
import static net.pincette.rs.Util.trace;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Logger;

/**
 * Base class for buffered processors. It uses a shared thread.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @since 3.0
 * @author Werner Donné
 */
public abstract class Buffered<T, R> extends ProcessorBase<T, R> {
  private final Deque<R> buf = new ArrayDeque<>(1000);
  private final Logger logger = getLogger(getClass().getName());
  private final long requestSize;
  private final Duration timeout;
  private boolean cancelled;
  private boolean completed;
  private boolean completedSent;
  private boolean lastRequested;
  private long received;
  private long requested;
  private long requestedUpstream;
  private boolean started;

  /**
   * Create a buffered processor. The timeout is set to 0.
   *
   * @param requestSize the number of elements that will be requested from the upstream.
   */
  protected Buffered(final int requestSize) {
    this(requestSize, null);
  }

  /**
   * Create a buffered processor.
   *
   * @param requestSize the number of elements that will be requested from the upstream.
   * @param timeout the time after which an additional element is requested, even if the upstream
   *     publisher hasn't sent all requested elements yet. This provides the opportunity to the
   *     publisher to complete properly when it has fewer elements left than the buffer size. If the
   *     timeout is zero, the additional element is requested immediately when not everything has
   *     been received yet. It may be <code>null</code>, in which case this behaviour will not
   *     occur.
   */
  Buffered(final int requestSize, final Duration timeout) {
    if (requestSize < 1) {
      throw new IllegalArgumentException("Request size should be at least 1.");
    }

    if (timeout != null && timeout.isNegative()) {
      throw new IllegalArgumentException("The timeout should be positive.");
    }

    this.requestSize = requestSize;
    this.timeout = timeout;
  }

  protected void addValues(final List<R> values) {
    trace(logger, () -> "addValues values: " + values);
    values.forEach(buf::addFirst);
  }

  @Override
  protected void cancelling() {
    dispatch(
        () -> {
          cancelled = true;
          doLast();
          emit();
        });

    super.cancelling();
  }

  private boolean done() {
    return completed && buf.isEmpty() && started;
  }

  private void doLast() {
    if (!lastRequested) {
      lastRequested = true;
      last();
    }
  }

  @Override
  protected void emit(final long number) {
    trace(logger, () -> "dispatch emit number: " + number);

    dispatch(
        () -> {
          trace(logger, () -> "emit number: " + number);
          started = true;
          requested += number;
          more();
          emit();
        });
  }

  /** Triggers the downstream emission flow. The <code>onNextAction</code> method could use this. */
  protected void emit() {
    trace(logger, () -> "dispatch emit");

    dispatch(
        () -> {
          trace(logger, () -> "emit");

          if (getRequested() > 0) {
            trace(logger, () -> "emit buf: " + buf);
            trace(logger, () -> "emit requested: " + getRequested());

            Util.nextValues(buf, getRequested())
                .ifPresentOrElse(
                    values -> {
                      requested -= values.size();
                      sendValues(values);
                    },
                    () -> {
                      if (done()) {
                        sendComplete();
                      }
                    });

            more();
          } else {
            if (done()) {
              sendComplete();
            }
          }
        });
  }

  /**
   * Returns the number of requested elements by the downstream.
   *
   * @return The requested elements number.
   */
  protected long getRequested() {
    return requested;
  }

  /**
   * Indicates whether the stream is cancelled.
   *
   * @return The cancellation status.
   */
  protected boolean isCancelled() {
    return cancelled;
  }

  /**
   * Indicates whether the stream is completed.
   *
   * @return The completion status.
   */
  protected boolean isCompleted() {
    return completed;
  }

  /**
   * This is called when the stream has completed. It provides subclasses with the opportunity to
   * flush any remaining data to the buffer.
   */
  protected void last() {
    // Optional for subclasses.
  }

  private void keepItGoing() {
    if (shouldWakeUp()) {
      trace(logger, () -> "shouldWakeUp");
      more(1);
    }
  }

  private void more() {
    trace(logger, () -> "dispatch more");

    dispatch(
        () -> {
          trace(logger, () -> "more");

          if (needMore()) {
            trace(logger, () -> "needMore");
            more(requestSize);
          } else if (timeout != null && timeout.isZero()) {
            keepItGoing();
          }
        });
  }

  private void more(final long size) {
    requestedUpstream += size;
    trace(logger, () -> "more requestedUpstream: " + requestedUpstream);
    trace(logger, () -> "more received: " + received);
    trace(logger, () -> "more subscription request: " + size);
    subscription.request(size);
  }

  private boolean needMore() {
    return !isCompleted()
        && !isCancelled()
        && received == requestedUpstream
        && buf.isEmpty()
        && getRequested() > 0;
  }

  @Override
  public void onComplete() {
    trace(logger, () -> "dispatch onComplete");

    dispatch(
        () -> {
          trace(logger, () -> "onComplete buf: " + buf);
          completed = true;
          doLast();

          if (done()) {
            trace(logger, () -> "sendComplete from onComplete");
            sendComplete();
          } else {
            emit();
          }
        });
  }

  @Override
  public void onError(final Throwable t) {
    if (t == null) {
      throw new NullPointerException("Can't throw null.");
    }

    dispatch(
        () -> {
          setError(true);
          subscriber.onError(t);
        });
  }

  @Override
  public void onNext(final T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (!getError()) {
      trace(logger, () -> "dispatch onNext value: " + value);

      dispatch(
          () -> {
            if (received == requestedUpstream) {
              throwBackpressureViolation(this, subscription, requestedUpstream);
            }

            ++received;
            trace(
                logger,
                () ->
                    "onNext received: "
                        + received
                        + ", requested upstream: "
                        + requestedUpstream
                        + ", buffer size: "
                        + buf.size());

            if (!onNextAction(value)) {
              trace(logger, () -> "onNext onNextAction false");
              more();
            }

            trace(logger, () -> "onNextAction buffer size: " + buf.size());
          });
    }
  }

  /**
   * The <code>onNext</code> method uses this method.
   *
   * @param value the received value.
   * @return Indicated if values have been added or not.
   */
  protected abstract boolean onNextAction(final T value);

  @Override
  public void onSubscribe(final Subscription subscription) {
    super.onSubscribe(subscription);

    if (timeout != null && !timeout.isZero()) {
      runRequestTimeout();
    }
  }

  private void runRequestTimeout() {
    runAsyncAfter(
        () ->
            dispatch(
                () -> {
                  if (!isCompleted() && !getError()) {
                    runRequestTimeout();
                    keepItGoing();
                  }
                }),
        timeout);
  }

  private void sendComplete() {
    trace(logger, () -> "dispatch sendComplete");

    dispatch(
        () -> {
          if (!completedSent) {
            completedSent = true;
            trace(logger, () -> "send onComplete");
            subscriber.onComplete();
          }
        });
  }

  /**
   * Sends the values to the downstream one by one.
   *
   * @param values the values to be sent.
   */
  private void sendValues(final List<R> values) {
    if (!getError()) {
      trace(logger, () -> "dispatch values: " + values);
      values.forEach(
          v ->
              dispatch(
                  () -> {
                    trace(logger, () -> "sendValue: " + v);
                    subscriber.onNext(v);
                  }));

      dispatch(
          () -> {
            if (completed) {
              doLast();

              if (buf.isEmpty()) {
                trace(logger, () -> "sendComplete from sendValues");
                sendComplete();
              }
            }
          });
    }
  }

  private boolean shouldWakeUp() {
    return !isCompleted()
        && !getError()
        && received < requestedUpstream
        && buf.size() < requestSize
        && getRequested() > 0;
  }
}
