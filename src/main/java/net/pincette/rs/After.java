package net.pincette.rs;

/**
 * A processor which emits a given value after all incoming values have been emitted.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class After<T> extends Mapper<T, T> {
  private final T value;

  public After(final T value) {
    super(v -> v);
    this.value = value;
  }

  @Override
  public void onComplete() {
    super.onNext(value);
    super.onComplete();
  }
}
