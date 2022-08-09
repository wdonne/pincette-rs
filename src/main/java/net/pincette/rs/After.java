package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
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
  private boolean completed;

  public After(final T value) {
    this(() -> value);
  }

  public After(final Supplier<T> value) {
    super(v -> v);
    this.value = value;
  }

  public static <T> Processor<T, T> after(final T value) {
    return new After<>(value);
  }

  public static <T> Processor<T, T> after(final Supplier<T> value) {
    return new After<>(value);
  }

  @Override
  protected boolean canRequestMore(long n) {
    return !completed;
  }

  @Override
  public void onComplete() {
    if (!completed) {
      completed = true;
      super.onNext(value.get());
      super.onComplete();
    }
  }
}
