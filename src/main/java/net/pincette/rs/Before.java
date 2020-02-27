package net.pincette.rs;

import java.util.function.Supplier;

/**
 * A processor which emits a given value before all incoming values have been emitted.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Before<T> extends Mapper<T, T> {
  private final Supplier<T> value;
  private boolean first = true;

  public Before(final T value) {
    this(() -> value);
  }

  public Before(final Supplier<T> value) {
    super(v -> v);
    this.value = value;
  }

  @Override
  public void onComplete() {
    if (first) {
      super.onNext(value.get());
    }

    super.onComplete();
  }

  @Override
  public void onNext(final T value) {
    if (first) {
      first = false;
      super.onNext(this.value.get());
    }

    super.onNext(value);
  }
}
