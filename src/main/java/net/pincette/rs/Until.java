package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.function.Predicate;

/**
 * Emits the values until it receives one that matches the predicate, which is also emitted.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 1.4
 */
public class Until<T> extends PassThrough<T> {
  private final Predicate<T> predicate;
  private boolean done;
  private boolean sentComplete;

  public Until(final Predicate<T> predicate) {
    this.predicate = predicate;
  }

  public static <T> Processor<T, T> until(final Predicate<T> predicate) {
    return new Until<>(predicate);
  }

  @Override
  public void onComplete() {
    if (!sentComplete) {
      sentComplete = true;
      super.onComplete();
    }
  }

  @Override
  public void onNext(final T value) {
    if (!done) {
      done = predicate.test(value);
      super.onNext(value);

      if (done) {
        complete();
      }
    }
  }
}
