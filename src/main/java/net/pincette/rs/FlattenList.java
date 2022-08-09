package net.pincette.rs;

import static net.pincette.rs.Box.box;
import static net.pincette.rs.Mapper.map;

import java.util.List;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;

/**
 * The processor emits the elements in the received lists individually. It uses a shared thread.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class FlattenList<T> extends Buffered<List<T>, T> {
  public FlattenList() {
    super(1);
  }

  /**
   * Returns a processor that emits the elements from the generated list individually.
   *
   * @param function the function that generates the list.
   * @param <T> the value type.
   * @return The processor
   */
  public static <T, R> Processor<T, R> flatMapList(final Function<T, List<R>> function) {
    return box(map(function), flattenList());
  }

  /**
   * Returns a processor that emits the elements in the received lists individually.
   *
   * @param <T> the value type.
   * @return The processor.
   */
  public static <T> Processor<List<T>, T> flattenList() {
    return new FlattenList<>();
  }

  protected boolean onNextAction(final List<T> list) {
    addValues(list);
    emit();

    return true;
  }
}
