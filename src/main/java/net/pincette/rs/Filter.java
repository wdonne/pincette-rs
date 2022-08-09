package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.function.Predicate;

/**
 * Filters elements based on a predicate.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.4
 */
public class Filter<T> extends Mapper<T, T> {
  /**
   * Create a filter with a predicate.
   *
   * @param predicate elements that match are published.
   */
  public Filter(final Predicate<T> predicate) {
    super(v -> predicate.test(v) ? v : null);
  }

  public static <T> Processor<T, T> filter(final Predicate<T> predicate) {
    return new Filter<>(predicate);
  }
}
