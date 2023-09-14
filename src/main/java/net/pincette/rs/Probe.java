package net.pincette.rs;

import static net.pincette.util.Util.tryToDo;

import java.util.concurrent.Flow.Processor;
import java.util.function.Consumer;

/**
 * The processor lets all values through. It calls the given functions when more values are
 * requested or are emitted, which allows you to probe the backpressure mechanism.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Probe<T> extends ProcessorBase<T, T> {
  private final Runnable complete;
  private final Consumer<Throwable> error;
  private final Consumer<Long> more;
  private final Consumer<T> value;

  public Probe(final Consumer<Long> more) {
    this(more, v -> {});
  }

  public Probe(final Consumer<Long> more, final Consumer<T> value) {
    this(more, value, () -> {});
  }

  public Probe(final Consumer<Long> more, final Consumer<T> value, final Runnable complete) {
    this(more, value, complete, t -> {});
  }

  public Probe(
      final Consumer<Long> more,
      final Consumer<T> value,
      final Runnable complete,
      final Consumer<Throwable> error) {
    this.more = more;
    this.value = value;
    this.complete = complete;
    this.error = error;
  }

  public Probe(final Runnable complete) {
    this(n -> {}, v -> {}, complete);
  }

  public static <T> Processor<T, T> probe(final Consumer<Long> more) {
    return new Probe<>(more);
  }

  public static <T> Processor<T, T> probe(final Consumer<Long> more, final Consumer<T> value) {
    return new Probe<>(more, value);
  }

  public static <T> Processor<T, T> probe(
      final Consumer<Long> more, final Consumer<T> value, final Runnable complete) {
    return new Probe<>(more, value, complete);
  }

  public static <T> Processor<T, T> probe(
      final Consumer<Long> more,
      final Consumer<T> value,
      final Runnable complete,
      final Consumer<Throwable> error) {
    return new Probe<>(more, value, complete, error);
  }

  public static <T> Processor<T, T> probe(final Runnable complete) {
    return new Probe<>(complete);
  }

  @Override
  protected void emit(final long number) {
    tryToDo(
        () -> {
          if (more != null) {
            more.accept(number);
          }

          subscription.request(number);
        },
        this::onError);
  }

  @Override
  public void onComplete() {
    tryToDo(
        () -> {
          if (complete != null) {
            complete.run();
          }

          super.onComplete();
        },
        this::onError);
  }

  @Override
  public void onError(final Throwable t) {
    if (error != null) {
      error.accept(t);
    }

    super.onError(t);
  }

  @Override
  public void onNext(final T item) {
    tryToDo(
        () -> {
          if (value != null) {
            value.accept(item);
          }

          subscriber.onNext(item);
        },
        this::onError);
  }
}
