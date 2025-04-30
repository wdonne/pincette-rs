package net.pincette.rs;

import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToDoRethrow;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import net.pincette.function.ConsumerWithException;
import net.pincette.function.RunnableWithException;

/**
 * Provides constructors to which lambdas can be given. It requests values from the received
 * subscription one by one.
 *
 * @param <T> the parameterized type.
 * @author Werner Donné
 * @since 1.0
 */
public class LambdaSubscriber<T> implements Subscriber<T> {
  private final RunnableWithException complete;
  private final ConsumerWithException<Throwable> error;
  private final ConsumerWithException<T> next;
  private final Function<T, CompletionStage<Void>> nextComplete;
  private final ConsumerWithException<Subscription> subscribe;
  private Subscription subscription;

  public LambdaSubscriber(final ConsumerWithException<T> next) {
    this(next, null, null, null, null);
  }

  public LambdaSubscriber(
      final ConsumerWithException<T> next, final RunnableWithException complete) {
    this(next, null, complete, null, null);
  }

  public LambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error) {
    this(next, null, complete, error, null);
  }

  public LambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error,
      final ConsumerWithException<Subscription> subscribe) {
    this(next, null, complete, error, subscribe);
  }

  private LambdaSubscriber(
      final ConsumerWithException<T> next,
      final Function<T, CompletionStage<Void>> nextComplete,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error,
      final ConsumerWithException<Subscription> subscribe) {
    this.next = next;
    this.nextComplete = nextComplete;
    this.complete = complete;
    this.error = error;
    this.subscribe = subscribe;
  }

  public static <T> Subscriber<T> lambdaSubscriber(final ConsumerWithException<T> next) {
    return new LambdaSubscriber<>(next, null, null, null, null);
  }

  public static <T> Subscriber<T> lambdaSubscriber(
      final ConsumerWithException<T> next, final RunnableWithException complete) {
    return new LambdaSubscriber<>(next, null, complete, null, null);
  }

  public static <T> Subscriber<T> lambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error) {
    return new LambdaSubscriber<>(next, null, complete, error, null);
  }

  public static <T> Subscriber<T> lambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error,
      final ConsumerWithException<Subscription> subscribe) {
    return new LambdaSubscriber<>(next, null, complete, error, subscribe);
  }

  public static <T> Subscriber<T> lambdaSubscriberAsync(
      final Function<T, CompletionStage<Void>> next) {
    return new LambdaSubscriber<>(null, next, null, null, null);
  }

  public static <T> Subscriber<T> lambdaSubscriberAsync(
      final Function<T, CompletionStage<Void>> next, final RunnableWithException complete) {
    return new LambdaSubscriber<>(null, next, complete, null, null);
  }

  public static <T> Subscriber<T> lambdaSubscriberAsync(
      final Function<T, CompletionStage<Void>> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error) {
    return new LambdaSubscriber<>(null, next, complete, error, null);
  }

  public static <T> Subscriber<T> lambdaSubscriberAsync(
      final Function<T, CompletionStage<Void>> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error,
      final ConsumerWithException<Subscription> subscribe) {
    return new LambdaSubscriber<>(null, next, complete, error, subscribe);
  }

  private void more() {
    subscription.request(1);
  }

  public void onComplete() {
    if (complete != null) {
      tryToDoRethrow(complete);
    }
  }

  public void onError(final Throwable t) {
    if (t == null) {
      throw new NullPointerException("Can't throw null.");
    }

    if (error != null) {
      tryToDoRethrow(() -> error.accept(t));
    } else {
      rethrow(t);
    }
  }

  public void onNext(final T o) {
    if (o == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (subscription != null) {
      if (next != null) {
        tryToDoRethrow(() -> next.accept(o));
        more();
      } else if (nextComplete != null) {
        nextComplete.apply(o).thenRun(this::more);
      } else {
        more();
      }
    }
  }

  public void onSubscribe(final Subscription s) {
    if (s == null) {
      throw new NullPointerException("A subscription can't be null.");
    }

    final Monitor monitor = new Monitor(s);

    if (subscription != null) {
      subscription.cancel();
    } else {
      if (subscribe != null) {
        tryToDoRethrow(() -> subscribe.accept(monitor));
      }

      subscription = s;

      if (!monitor.cancelled && !monitor.requested) {
        s.request(1);
      }
    }
  }

  private static class Monitor implements Subscription {
    private final Subscription subscription;
    private boolean cancelled;
    private boolean requested;

    private Monitor(final Subscription subscription) {
      this.subscription = subscription;
    }

    public void cancel() {
      cancelled = true;
      subscription.cancel();
    }

    public void request(final long n) {
      requested = true;
      subscription.request(n);
    }
  }
}
