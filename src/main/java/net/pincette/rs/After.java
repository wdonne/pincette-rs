package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;

/**
 * A processor which emits a given value after all incoming values have been emitted.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 1.0
 */
public class After<T> extends Buffered<T, T> {
  private final Supplier<T> value;

  public After(final T value) {
    this(() -> value);
  }

  public After(final Supplier<T> value) {
    super(1);
    this.value = value;
  }

  public static <T> Processor<T, T> after(final T value) {
    return new After<>(value);
  }

  public static <T> Processor<T, T> after(final Supplier<T> value) {
    return new After<>(value);
  }

  @Override
  public void last() {
    addValues(list(value.get()));
  }

  @Override
  protected boolean onNextAction(final T value) {
    addValues(list(value));
    emit();

    return true;
  }
}
