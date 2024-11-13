package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.function.Function;

/**
 * In cases it makes sense to produce a message despite the occurrence of an exception, you can do
 * that with this processor.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.7.0
 */
public class CatchError<T> extends ProcessorBase<T, T> {
  private final Function<Throwable, T> function;

  public CatchError(Function<Throwable, T> function) {
    this.function = function;
  }

  public static <T> Processor<T, T> catchError(Function<Throwable, T> function) {
    return new CatchError<>(function);
  }

  @Override
  protected void emit(final long number) {
    subscription.request(number);
  }

  @Override
  public void onError(final Throwable throwable) {
    onNext(function.apply(throwable));
  }

  @Override
  public void onNext(final T item) {
    subscriber.onNext(item);
  }
}
