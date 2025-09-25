package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.function.Predicate;

/**
 * Filters elements based on the negation opf a predicate.
 *
 * @param <T> the value type.
 * @author Werner Donné
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

  public static <T> Processor<T, T> notFilter(final Predicate<T> predicate) {
    return new NotFilter<>(predicate);
  }
}
