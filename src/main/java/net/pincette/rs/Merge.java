package net.pincette.rs;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.QueuePublisher.queuePublisher;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * A publisher that emits everything that the given publishers emit.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.0
 */
public class Merge<T> implements Publisher<T> {
  private static final int BATCH_SIZE = 100;
  private static final Logger LOGGER = getLogger(Merge.class.getName());
  private static final Duration TIMEOUT = ofMillis(50);

  private final List<BranchSubscriber> branchSubscribers;
  private final String key = UUID.randomUUID().toString();
  private final QueuePublisher<T> publisher = queuePublisher((p, n) -> more(), p -> cancel());
  private final Consumer<Supplier<String>> tracer;

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   */
  public Merge(final List<? extends Publisher<T>> publishers) {
    this(publishers, false);
  }

  Merge(final List<? extends Publisher<T>> publishers, final boolean traceSpecific) {
    tracer = Util.tracer(LOGGER, this, traceSpecific);
    branchSubscribers = publishers.stream().map(this::branchSubscriber).toList();
  }

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   * @param <T> the value type.
   * @return The new publisher.
   */
  public static <T> Publisher<T> of(final List<? extends Publisher<T>> publishers) {
    return new Merge<>(publishers);
  }

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   * @param <T> the value type.
   * @return The new publisher.
   */
  @SafeVarargs
  public static <T> Publisher<T> of(final Publisher<T>... publishers) {
    return new Merge<>(asList(publishers));
  }

  private BranchSubscriber branchSubscriber(final Publisher<T> publisher) {
    final BranchSubscriber s = new BranchSubscriber();

    tracer.accept(() -> s + ": subscribe to " + publisher);
    with(publisher).per(BATCH_SIZE, TIMEOUT).get().subscribe(s);

    return s;
  }

  private void cancel() {
    branchSubscribers.forEach(BranchSubscriber::cancel);
  }

  private void more() {
    branchSubscribers.stream().filter(s -> !s.complete).forEach(BranchSubscriber::more);
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    publisher.subscribe(subscriber);
  }

  private class BranchSubscriber implements Subscriber<List<T>> {
    private boolean complete;
    private long pendingRequests;
    private Subscription subscription;

    private boolean allCompleted() {
      return branchSubscribers.stream().allMatch(s -> s.complete);
    }

    private void cancel() {
      subscription.cancel();
    }

    private void cancelOthers() {
      branchSubscribers.stream()
          .filter(s -> s != this)
          .map(s -> s.subscription)
          .forEach(Subscription::cancel);
    }

    private void closeQueue() {
      dispatch(
          () -> {
            if (!publisher.isClosed()) {
              tracer.accept(() -> "Close queue " + this);
              publisher.close();
            }
          });
    }

    private void complete() {
      dispatch(
          () -> {
            complete = true;

            if (allCompleted()) {
              closeQueue();
            }
          });
    }

    private void dispatch(final Runnable action) {
      Serializer.dispatch(action::run, this::onError, key);
    }

    private void more() {
      dispatch(
          () -> {
            if (subscription != null) {
              subscription.request(1);
            } else {
              ++pendingRequests;
            }
          });
    }

    public void onComplete() {
      tracer.accept(() -> "onComplete for subscriber " + this);
      complete();
    }

    public void onError(final Throwable t) {
      LOGGER.severe(() -> onErrorMessage(t));
      publisher.onError(t);
      cancelOthers();
    }

    private String onErrorMessage(final Throwable t) {
      return this + ": onError: " + t + "\ncomplete: " + complete + "\n";
    }

    public void onNext(final List<T> item) {
      dispatch(
          () -> {
            if (!publisher.isClosed()) {
              tracer.accept(() -> "Add onNext to queue " + this + ": " + item);
              publisher.getQueue().addAll(item);
            }
          });
    }

    public void onSubscribe(final Subscription subscription) {
      if (subscription == null) {
        throw new NullPointerException("A subscription can't be null.");
      }

      if (this.subscription != null) {
        subscription.cancel();
      }

      tracer.accept(() -> this + ": onSubscribe: " + subscription);
      this.subscription = subscription;

      dispatch(
          () -> {
            if (pendingRequests > 0) {
              subscription.request(pendingRequests);
              pendingRequests = 0;
            }
          });
    }
  }
}
