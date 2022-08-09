package net.pincette.rs;

import static net.pincette.rs.Serializer.dispatch;

import java.util.concurrent.Flow.Processor;

/**
 * When the upstream or downstream could cause races, this processor serializes everything. It uses
 * a shared thread.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Split<T> extends ProcessorBase<T, T> {
  public static <T> Processor<T, T> split() {
    return new Split<>();
  }

  @Override
  protected void emit(long number) {
    dispatch(() -> subscription.request(number));
  }

  @Override
  public void onComplete() {
    dispatch(super::onComplete);
  }

  @Override
  public void onError(Throwable t) {
    dispatch(() -> super.onError(t));
  }

  @Override
  public void onNext(T value) {
    dispatch(() -> subscriber.onNext(value));
  }
}
