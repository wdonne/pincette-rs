package net.pincette.rs;

import java.util.function.Supplier;

/**
 * A processor which emits a given value after all incoming values have been emitted.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class After<T> extends Mapper<T, T> {
  private final Supplier<T> value;
  private boolean extra;

  public After(final T value) {
    this(() -> value);
  }

  public After(final Supplier<T> value) {
    super(v -> v);
    this.value = value;
  }

  @Override
  protected boolean canRequestMore(long n) {
    if (extra) {
      extra = false;

      return false;
    }

    return true;
  }

  @Override
  public void onComplete() {
    extra = true;
    super.onNext(value.get());
    super.onComplete();
  }
}
