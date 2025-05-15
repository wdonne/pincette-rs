package net.pincette.rs;

import java.util.concurrent.Flow.Processor;

/**
 * A processor which just passes through all values.
 *
 * @param <T> the object type of the values.
 * @author Werner Donné
 * @since 1.0
 */
public class PassThrough<T> extends Mapper<T, T> {
  public PassThrough() {
    super(v -> v);
  }

  /**
   * Returns a <code>PassThrough</code> processor.
   *
   * @param <T> the value type.
   * @return The processor.
   * @since 3.0
   */
  public static <T> Processor<T, T> passThrough() {
    return new PassThrough<>();
  }
}
