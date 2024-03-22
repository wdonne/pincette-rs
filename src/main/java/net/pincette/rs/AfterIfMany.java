package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;

/**
 * A processor which emits a given value after all incoming values have been emitted, only when
 * there is more than one event.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.5
 */
public class AfterIfMany<T> extends Buffered<T, T> {
  private final Supplier<T> value;
  private int seen;

  public AfterIfMany(final T value) {
    this(() -> value);
  }

  public AfterIfMany(final Supplier<T> value) {
    super(1);
    this.value = value;
  }

  public static <T> Processor<T, T> afterIfMany(final T value) {
    return new AfterIfMany<>(value);
  }

  public static <T> Processor<T, T> afterIfMany(final Supplier<T> value) {
    return new AfterIfMany<>(value);
  }

  @Override
  public void last() {
    if (seen > 1) {
      addValues(list(value.get()));
    }
  }

  @Override
  protected boolean onNextAction(final T value) {
    ++seen;
    addValues(list(value));
    emit();

    return true;
  }
}
