package net.pincette.rs;

import java.util.function.Predicate;

/**
 * Emits the values until it receives one that matches the predicate, which is also emitted.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.4
 */
public class Until<T> extends PassThrough<T> {
  private final Predicate<T> predicate;
  private boolean done;

  public Until(final Predicate<T> predicate) {
    this.predicate = predicate;
  }

  @Override
  public void onNext(final T value) {
    if (!done) {
      done = predicate.test(value);
      super.onNext(value);
    } else {
      complete();
    }
  }
}
