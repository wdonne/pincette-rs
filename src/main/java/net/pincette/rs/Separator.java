package net.pincette.rs;

import java.util.function.Supplier;

/**
 * A processor which emits a given value between the incoming value stream.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Separator<T> extends Mapper<T, T> {
  private final Supplier<T> value;
  private boolean extra;
  private boolean first = true;

  public Separator(final T value) {
    this(() -> value);
  }

  public Separator(final Supplier<T> value) {
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
  public void onNext(final T value) {
    if (first) {
      first = false;
    } else {
      extra = true;
      super.onNext(this.value.get());
    }

    super.onNext(value);
  }
}
