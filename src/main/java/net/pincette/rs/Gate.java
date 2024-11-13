package net.pincette.rs;

import static java.lang.Math.min;

import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;

/**
 * With this processor you can control the backpressure rate with external conditions. The number of
 * requests will neither exceed the natural request rate nor the maximum allowed requests, defined
 * by the external function.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.7.0
 */
public class Gate<T> extends ProcessorBase<T, T> {
  private final Supplier<Long> allowed;
  private long requests;

  public Gate(final Supplier<Long> allowed) {
    this.allowed = allowed;
  }

  public static <T> Processor<T, T> gate(final Supplier<Long> allowed) {
    return new Gate<>(allowed);
  }

  @Override
  protected void emit(final long number) {
    dispatch(
        () -> {
          requests += number;

          final long asking = min(requests, allowed.get());

          if (asking > 0) {
            requests -= asking;
            subscription.request(asking);
          }
        });
  }

  @Override
  public void onNext(final T item) {
    subscriber.onNext(item);
  }
}
