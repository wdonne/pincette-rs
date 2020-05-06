package net.pincette.rs;

/**
 * Emits the last value it receives and then completes the stream.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.4
 */
public class Last<T> extends PassThrough<T> {
  private T lastValue;

  @Override
  public void onComplete() {
    if (lastValue != null) {
      super.onNext(lastValue);
      super.onComplete();
    }
  }

  @Override
  public void onNext(final T value) {
    lastValue = value;
    more();
  }
}
