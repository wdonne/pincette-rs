package net.pincette.rs;

/**
 * A processor which emits a given value between the incoming value stream.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Separator<T> extends Mapper<T, T> {
  private final T value;
  private boolean first = true;

  public Separator(final T value) {
    super(v -> v);
    this.value = value;
  }

  @Override
  public void onNext(final T value) {
    if (first) {
      first = false;
    } else {
      super.onNext(this.value);
    }

    super.onNext(value);
  }
}
