package net.pincette.rs;

import java.util.function.Predicate;

/**
 * Filters elements based on the negation opf a predicate.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.4
 */
public class NotFilter<T> extends Mapper<T, T> {
  /**
   * Create a filter with a predicate.
   *
   * @param predicate elements that don't match are published.
   */
  public NotFilter(final Predicate<T> predicate) {
    super(v -> !predicate.test(v) ? v : null);
  }
}
