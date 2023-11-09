package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;

/**
 * A processor which emits a given value before all incoming values have been emitted.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 1.0
 */
public class Before<T> extends Buffered<T, T> {
  private final Supplier<T> value;
  private boolean first = true;

  public Before(final T value) {
    this(() -> value);
  }

  public Before(final Supplier<T> value) {
    super(1);
    this.value = value;
  }

  public static <T> Processor<T, T> before(final T value) {
    return new Before<>(value);
  }

  public static <T> Processor<T, T> before(final Supplier<T> value) {
    return new Before<>(value);
  }

  @Override
  public void last() {
    if (first) {
      addValues(list(value.get()));
    }
  }

  @Override
  protected boolean onNextAction(final T value) {
    final List<T> values = new ArrayList<>();

    if (first) {
      first = false;
      values.add(this.value.get());
    }

    values.add(value);
    addValues(values);
    emit();

    return true;
  }
}
