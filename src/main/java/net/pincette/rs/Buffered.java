package net.pincette.rs;

import static net.pincette.rs.Util.LOGGER;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;
import net.pincette.util.Util.GeneralException;

/**
 * Base class for buffered processors. It uses a shared thread.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @since 3.0
 * @author Werner Donn\u00e8
 */
public abstract class Buffered<T, R> extends ProcessorBase<T, R> {
  private final Deque<R> buf = new ConcurrentLinkedDeque<>();
  private final long requestSize;
  private boolean completed;
  private boolean completedSent;
  private boolean lastRequested;
  private long received;
  private long requested;
  private long requestedUpstream;

  /**
   * Create a buffered processor.
   *
   * @param requestSize the number of elements that will be requested from the upstream.
   */
  protected Buffered(final int requestSize) {
    if (requestSize < 1) {
      throw new IllegalArgumentException("Request size should be at least 1.");
    }

    this.requestSize = requestSize;
  }

  protected void addValues(final List<R> values) {
    trace(() -> "addValues values: " + values);
    values.forEach(buf::addFirst);
  }

  protected void dispatch(final Runnable action) {
    Serializer.dispatch(action);
  }

  private boolean done() {
    return completed && (received == 0 || buf.isEmpty());
  }

  private void doLast() {
    if (!lastRequested) {
      lastRequested = true;
      last();
    }
  }

  @Override
  protected void emit(final long number) {
    trace(() -> "dispatch emit number: " + number);

    dispatch(
        () -> {
          trace(() -> "emit number: " + number);
          requested += number;
          more();
          emit();
        });
  }

  /** Triggers the downstream emission flow. The <code>onNextAction</code> method could use this. */
  protected void emit() {
    trace(() -> "dispatch emit");

    dispatch(
        () -> {
          trace(() -> "emit");

          if (getRequested() > 0) {
            trace(() -> "emit buf: " + buf);
            trace(() -> "emit requested: " + getRequested());

            Util.nextValues(buf, getRequested())
                .ifPresent(
                    values -> {
                      requested -= values.size();
                      sendValues(values);
                    });

            more();
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
   * Indicates whether the stream is completed.
   *
   * @return The completes status.
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

  private void more() {
    trace(() -> "dispatch more");

    dispatch(
        () -> {
          trace(() -> "more");

          if (needMore()) {
            requestedUpstream += requestSize;
            trace(() -> "more requestedUpstream: " + requestedUpstream);
            trace(() -> "more subscription request: " + requestSize);
            subscription.request(requestSize);
          }
        });
  }

  private boolean needMore() {
    return !isCompleted() && received == requestedUpstream && getRequested() > buf.size();
  }

  @Override
  public void onComplete() {
    trace(() -> "dispatch onComplete");

    dispatch(
        () -> {
          trace(() -> "onComplete buf: " + buf);
          completed = true;
          doLast();

          if (done()) {
            trace(() -> "sendComplete from onComplete");
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

    setError(true);
    subscriber.onError(t);
  }

  @Override
  public void onNext(final T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (!getError()) {
      if (received == requestedUpstream) {
        throw new GeneralException(
            "Backpressure violation in "
                + subscription.getClass().getName()
                + ". Requested "
                + requestedUpstream
                + " elements in "
                + getClass().getName()
                + ", which have already been received.");
      }

      trace(() -> "dispatch onNext value: " + value);

      dispatch(
          () -> {
            ++received;
            trace(() -> "onNext received: " + received);

            if (!onNextAction(value)) {
              trace(() -> "onNext onNextAction false");
              more();
            }
          });
    }
  }

  /**
   * The <code>onNext</code> method uses this method.
   *
   * @param value the received value.
   */
  protected abstract boolean onNextAction(final T value);

  private void sendComplete() {
    trace(() -> "dispatch sendComplete");

    dispatch(
        () -> {
          if (!completedSent) {
            completedSent = true;
            trace(() -> "send onComplete");
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
      trace(() -> "dispatch values: " + values);
      values.forEach(
          v ->
              dispatch(
                  () -> {
                    trace(() -> "sendValue: " + v);
                    subscriber.onNext(v);
                  }));

      dispatch(
          () -> {
            if (completed) {
              doLast();

              if (buf.isEmpty()) {
                trace(() -> "sendComplete from sendValues");
                sendComplete();
              }
            }
          });
    }
  }

  private void trace(final Supplier<String> message) {
    LOGGER.finest(() -> getClass().getName() + ": " + message.get());
  }
}