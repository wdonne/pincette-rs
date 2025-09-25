package net.pincette.rs;

import java.util.concurrent.Flow.Processor;

/**
 * Emits the last value it receives and then completes the stream.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 1.4
 */
public class Last<T> extends PassThrough<T> {
  private T lastValue;

  public static <T> Processor<T, T> last() {
    return new Last<>();
  }

  @Override
  public void onComplete() {
    if (lastValue != null) {
      super.onNext(lastValue);
    }

    super.onComplete();
  }

  @Override
  public void onNext(final T value) {
    lastValue = value;
    more();
  }
}
