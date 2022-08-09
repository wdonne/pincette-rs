package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
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
  private boolean extra;
  private boolean first = true;

  public Before(final T value) {
    this(() -> value);
  }

  public Before(final Supplier<T> value) {
    super(v -> v);
    this.value = value;
  }

  public static <T> Processor<T, T> before(final T value) {
    return new Before<>(value);
  }

  public static <T> Processor<T, T> before(final Supplier<T> value) {
    return new Before<>(value);
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
    if (first) {
      super.onNext(value.get());
    }

    super.onComplete();
  }

  @Override
  public void onNext(final T value) {
    if (first) {
      first = false;
      extra = true;
      super.onNext(this.value.get());
    }

    super.onNext(value);
  }
}
