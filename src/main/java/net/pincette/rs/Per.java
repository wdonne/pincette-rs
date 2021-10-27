package net.pincette.rs;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.List;

/**
 * Buffers a number of values. It always requests the number of values from the publisher that
 * equals the buffer size. It emits the buffered values as a list.
 *
 * @param <T> the value type.
 * @since 2.0
 * @author Werner Donn\u00e8
 */
public class Per<T> extends Buffered<T, List<T>> {
  /**
   * Create a buffer of <code>size</code>.
   *
   * @param size the buffer size, which must be larger than zero.
   */
  public Per(final int size) {
    super(
        size,
        buf -> {
          final int toDo = min(buf.size(), size);
          final List<T> result = new ArrayList<>(toDo);

          for (int i = 0; i < toDo; ++i) {
            result.add(buf.removeLast());
          }

          return result;
        });
  }
}
