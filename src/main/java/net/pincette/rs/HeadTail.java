package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A processor that doesn't emit the head of a stream, but instead gives it to a function.
 *
 * @param <T> the type of the incoming values.
 * @param <R> the type of the outgoing values.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class HeadTail<T, R> extends Mapper<T, R> {
  private final Consumer<T> head;
  private boolean headSeen;

  /**
   * Creates the processor.
   *
   * @param head the function that receives the first value.
   * @param tail the function that receives all other values.
   */
  public HeadTail(final Consumer<T> head, final Function<T, R> tail) {
    super(tail);
    this.head = head;
  }

  public static <T, R> Processor<T, R> headTail(final Consumer<T> head, final Function<T, R> tail) {
    return new HeadTail<>(head, tail);
  }

  @Override
  public void onNext(final T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (!headSeen) {
      headSeen = true;
      head.accept(value);
      more();
    } else {
      super.onNext(value);
    }
  }
}
