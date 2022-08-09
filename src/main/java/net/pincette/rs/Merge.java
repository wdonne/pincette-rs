package net.pincette.rs;

import static java.lang.Math.max;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static java.util.stream.Collectors.toList;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Util.LOGGER;

import java.util.List;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;

/**
 * A publisher that emits everything that the given publishers emit.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Merge<T> implements Publisher<T> {
  private final List<BranchSubscriber> branchSubscribers;
  private final Processor<T, T> buffer;
  private boolean completed;
  private Subscriber<? super T> subscriber;

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   */
  public Merge(final List<Publisher<T>> publishers) {
    buffer = buffer(max(1000, publishers.size()));
    branchSubscribers = publishers.stream().map(this::branchSubscriber).collect(toList());
  }

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   * @param <T> the value type.
   * @return The new publisher.
   */
  public static <T> Publisher<T> of(final List<Publisher<T>> publishers) {
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

  private void trace(final Supplier<String> message) {
    LOGGER.finest(() -> getClass().getName() + ": " + message.get());
  }

  private boolean allSubscriptions() {
    return branchSubscribers.stream().allMatch(s -> s.subscription != null);
  }

  private BranchSubscriber branchSubscriber(final Publisher<T> publisher) {
    final BranchSubscriber s = new BranchSubscriber();

    publisher.subscribe(s);

    return s;
  }

  private void notifySubscriber() {
    if (subscriber != null && allSubscriptions()) {
      buffer.onSubscribe(new Backpressure());
      buffer.subscribe(subscriber);
    }
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;
    notifySubscriber();
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      branchSubscribers.forEach(b -> b.subscription.cancel());
    }

    private List<BranchSubscriber> incomplete() {
      return branchSubscribers.stream().filter(s -> !s.complete).collect(toList());
    }

    public void request(final long n) {
      if (n <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      trace(() -> "request: " + n);
      if (!completed) {
        requestBranches(n);
      }
    }

    private void requestBranches(final long n) {
      final List<BranchSubscriber> incomplete = incomplete();

      if (!incomplete.isEmpty()) {
        final long[] requests = spreadRequests(n, incomplete.size());

        for (int i = 0; i < requests.length; ++i) {
          final long num = requests[i];

          trace(() -> "branch request: " + num);
          incomplete.get(i).subscription.request(num);
        }
      }
    }

    private long[] spreadRequests(final long n, final int numberSubscribers) {
      final long[] result = new long[numberSubscribers];

      fill(result, 1);

      long remaining = n - result.length;

      while (remaining > 0) {
        for (int i = 0; i < result.length && remaining > 0; ++i, --remaining) {
          result[i] += 1;
        }
      }

      return result;
    }
  }

  private class BranchSubscriber implements Subscriber<T> {
    private boolean complete;
    private Subscription subscription;

    private boolean allCompleted() {
      return branchSubscribers.stream().allMatch(s -> s.complete);
    }

    private void cancelOthers() {
      branchSubscribers.stream()
          .filter(s -> s != this)
          .map(s -> s.subscription)
          .forEach(Subscription::cancel);
    }

    private void complete() {
      complete = true;

      if (!completed && allCompleted()) {
        completed = true;
        trace(() -> "Send onComplete to buffer");
        buffer.onComplete();
      }
    }

    public void onComplete() {
      trace(() -> "onComplete");
      complete();
    }

    public void onError(final Throwable throwable) {
      buffer.onError(throwable);
      cancelOthers();
    }

    public void onNext(final T item) {
      trace(() -> "Send onNext to buffer: " + item);
      buffer.onNext(item);
    }

    public void onSubscribe(final Subscription subscription) {
      if (subscription == null) {
        throw new NullPointerException("A subscription can't be null.");
      }

      if (this.subscription != null) {
        subscription.cancel();
      } else {
        this.subscription = subscription;
        notifySubscriber();
      }
    }
  }
}
