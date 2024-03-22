package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;

/**
 * A processor which emits a given value before all incoming values have been emitted, only when
 * there is more than one event.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.5
 */
public class BeforeIfMany<T> extends Buffered<T, T> {
  private final Supplier<T> value;
  private boolean first = true;
  private T firstValue;

  public BeforeIfMany(final T value) {
    this(() -> value);
  }

  public BeforeIfMany(final Supplier<T> value) {
    super(1);
    this.value = value;
  }

  public static <T> Processor<T, T> beforeIfMany(final T value) {
    return new BeforeIfMany<>(value);
  }

  public static <T> Processor<T, T> beforeIfMany(final Supplier<T> value) {
    return new BeforeIfMany<>(value);
  }

  @Override
  public void last() {
    if (firstValue != null) {
      addValues(list(firstValue));
    }
  }

  @Override
  protected boolean onNextAction(final T value) {
    final List<T> values = new ArrayList<>();

    if (first && firstValue == null) {
      firstValue = value;

      return false;
    }

    if (first) {
      first = false;
      values.add(this.value.get());
      values.add(firstValue);
      firstValue = null;
    }

    values.add(value);
    addValues(values);
    emit();

    return true;
  }
}
