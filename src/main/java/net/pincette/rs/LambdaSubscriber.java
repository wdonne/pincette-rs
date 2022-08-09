package net.pincette.rs;

import static net.pincette.util.Util.tryToDoRethrow;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import net.pincette.function.ConsumerWithException;
import net.pincette.function.RunnableWithException;
import net.pincette.util.Util.GeneralException;

/**
 * Provides constructors to which lambdas can be given. It requests values from the received
 * subscription one by one.
 *
 * @param <T> the parameterized type.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class LambdaSubscriber<T> implements Subscriber<T> {
  private final RunnableWithException complete;
  private final ConsumerWithException<Throwable> error;
  private final ConsumerWithException<T> next;
  private final ConsumerWithException<Subscription> subscribe;
  private Subscription subscription;

  public LambdaSubscriber(final ConsumerWithException<T> next) {
    this(next, null, null, null);
  }

  public LambdaSubscriber(
      final ConsumerWithException<T> next, final RunnableWithException complete) {
    this(next, complete, null, null);
  }

  public LambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error) {
    this(next, complete, error, null);
  }

  public LambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error,
      final ConsumerWithException<Subscription> subscribe) {
    this.next = next;
    this.complete = complete;
    this.error = error;
    this.subscribe = subscribe;
  }

  public static <T> Subscriber<T> lambdaSubscriber(final ConsumerWithException<T> next) {
    return new LambdaSubscriber<>(next);
  }

  public static <T> Subscriber<T> lambdaSubscriber(
      final ConsumerWithException<T> next, final RunnableWithException complete) {
    return new LambdaSubscriber<>(next, complete);
  }

  public static <T> Subscriber<T> lambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error) {
    return new LambdaSubscriber<>(next, complete, error);
  }

  public static <T> Subscriber<T> lambdaSubscriber(
      final ConsumerWithException<T> next,
      final RunnableWithException complete,
      final ConsumerWithException<Throwable> error,
      final ConsumerWithException<Subscription> subscribe) {
    return new LambdaSubscriber<>(next, complete, error, subscribe);
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
      throw new GeneralException(t);
    }
  }

  public void onNext(final T o) {
    if (o == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (subscription != null) {
      if (next != null) {
        tryToDoRethrow(() -> next.accept(o));
      }

      subscription.request(1);
    }
  }

  public void onSubscribe(final Subscription s) {
    if (s == null) {
      throw new NullPointerException("A subscription can't be null.");
    }

    if (this.subscription != null) {
      s.cancel();
    } else {
      if (subscribe != null) {
        tryToDoRethrow(() -> subscribe.accept(s));
      }

      subscription = s;
      s.request(1);
    }
  }
}
