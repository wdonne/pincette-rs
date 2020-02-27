package net.pincette.rs;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

/**
 * Chains processors after the initial publisher.
 *
 * @param <T> the object type of the initial publisher.
 * @author Werner DOnn\u00e9
 * @since 1.0
 */
public class Chain<T> {
  private final Publisher<T> publisher;

  private Chain(final Publisher<T> publisher) {
    this.publisher = publisher;
  }

  /**
   * Creates a chain with the initial <code>publisher</code>.
   *
   * @param publisher the initial publisher.
   * @param <T> the object type of the initial publisher.
   * @return The chain.
   * @since 1.0
   */
  public static <T> Chain<T> with(final Publisher<T> publisher) {
    return new Chain<>(publisher);
  }

  /**
   * Appends <code>value</code> to the stream.
   *
   * @param value the value to emit. It may be <code>null</code>.
   * @return the new stream.
   * @since 1.0
   */
  public Chain<T> after(final T value) {
    return map(new After<>(value));
  }

  /**
   * Appends the result of <code>value</code> to the stream.
   *
   * @param value the function that produces the value to emit. It may not be <code>null</code>.
   * @return the new stream.
   * @since 1.2.1
   */
  public Chain<T> after(final Supplier<T> value) {
    return map(new After<>(value));
  }

  /**
   * Puts <code>value</code> before the stream.
   *
   * @param value the value to emit. It may be <code>null</code>.
   * @return the new stream.
   * @since 1.0
   */
  public Chain<T> before(final T value) {
    return map(new Before<>(value));
  }

  /**
   * Puts the result of <code>value</code> before the stream.
   *
   * @param value the function that produces the value to emit. It may not be <code>null</code>.
   * @return the new stream.
   * @since 1.2.1
   */
  public Chain<T> before(final Supplier<T> value) {
    return map(new Before<>(value));
  }

  /**
   * Appends a processor that filters objects using the <code>predicate</code> function.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.0
   */
  public Chain<T> filter(final Predicate<T> predicate) {
    return map(v -> predicate.test(v) ? v : null);
  }

  /**
   * Returns the publisher of the chain.
   *
   * @return The publisher.
   * @since 1.0
   */
  public Publisher<T> get() {
    return publisher;
  }

  /**
   * Appends <code>processor</code> to the chain.
   *
   * @param processor the given processor.
   * @param <R> the object type for the new chain.
   * @return The new chain.
   * @since 1.0
   */
  public <R> Chain<R> map(final Processor<T, R> processor) {
    publisher.subscribe(processor);

    return new Chain<>(processor);
  }

  /**
   * Appends a processor with the mapping <code>function</code>, which transforms the objects.
   *
   * @param function the mapping function.
   * @param <R> the object type for the new chain.
   * @return The new chain.
   * @since 1.0
   */
  public <R> Chain<R> map(final Function<T, R> function) {
    return map(new Mapper<>(function));
  }

  /**
   * Appends a processor that filters objects using negation of the <code>predicate</code> function.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.0
   */
  public Chain<T> notFilter(final Predicate<T> predicate) {
    return map(v -> !predicate.test(v) ? v : null);
  }

  /**
   * Puts <code>value</code> between the emitted values.
   *
   * @param value the value to emit between the emitted values. It may be <code>null</code>.
   * @return The new stream.
   * @since 1.0
   */
  public Chain<T> separate(final T value) {
    return map(new Separator<>(value));
  }

  /**
   * Puts the result of <code>value</code> between the emitted values.
   *
   * @param value the function that produces the value to emit between the emitted values. It may
   *     not be <code>null</code>.
   * @return The new stream.
   * @since 1.2.1
   */
  public Chain<T> separate(final Supplier<T> value) {
    return map(new Separator<>(value));
  }
}
