package net.pincette.rs;

import static java.util.Optional.ofNullable;
import static net.pincette.util.Util.tryToGet;

import java.util.concurrent.Flow.Processor;
import java.util.function.Function;
import net.pincette.function.SideEffect;

/**
 * Transforms a reactive stream. Null values are neither processed nor emitted.
 *
 * @param <T> the type of the incoming values.
 * @param <R> the type of the outgoing values.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Mapper<T, R> extends ProcessorBase<T, R> {
  private final Function<T, R> map;

  public Mapper(final Function<T, R> map) {
    this.map = map;
  }

  /**
   * Returns a mapping processor with a map function.
   *
   * @param map the map function.
   * @param <T> the type of the incoming values.
   * @param <R> the type of the outgoing values.
   * @return The processor.
   * @since 3.0
   */
  public static <T, R> Processor<T, R> map(final Function<T, R> map) {
    return new Mapper<>(map);
  }

  /**
   * With this method a subclass can regulate the backpressure.
   *
   * @param number the strictly positive number of elements.
   * @return <code>true</code>
   * @since 1.6
   */
  protected boolean canRequestMore(long number) {
    return true;
  }

  @Override
  protected void emit(final long number) {
    more(number);
  }

  /**
   * Request one more element from the upstream.
   *
   * @since 1.4
   */
  protected void more() {
    more(1);
  }

  /**
   * Request more elements from the upstream. This will only have an effect when the stream is not
   * in an error state.
   *
   * @param number the strictly positive number of elements.
   * @since 1.4
   */
  protected void more(final long number) {
    if (number > 0 && !getError() && canRequestMore(number)) {
      subscription.request(number);
    }
  }

  private R newValue(final T value) {
    return ofNullable(value)
        .flatMap(
            v ->
                tryToGet(
                    () -> map.apply(v),
                    e ->
                        SideEffect.<R>run(
                                () -> {
                                  onError(e);
                                  cancel();
                                })
                            .andThenGet(() -> null)))
        .orElse(null);
  }

  @Override
  public void onNext(final T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (!getError()) {
      final R newValue = newValue(value);

      if (newValue != null) {
        subscriber.onNext(newValue);
      } else {
        more(); // Keep the upstream alive.
      }
    }
  }
}
