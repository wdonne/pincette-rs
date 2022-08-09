package net.pincette.rs;

import java.util.List;

/**
 * The interface for stateful encoders op value streams.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public interface Encoder<T, R> {
  /**
   * This method is called when the stream has completed. It provides the opportunity to generate
   * the last values from remaining state.
   *
   * @return The last values.
   */
  List<R> complete();

  /**
   * This method is called for each incoming value.
   *
   * @param value the value.
   * @return The list of generated values.
   */
  List<R> encode(final T value);
}
