package net.pincette.rs;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

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
   * Emits the values produced by the incoming completion stages in the order the stages arrive. The
   * streams completes only after the last stage has completed.
   *
   * @return the new stream.
   * @since 1.5
   * @deprecated Use {@link #mapAsync(Function)} instead.
   */
  @Deprecated(since = "1.5.1")
  public Chain<T> async() {
    return map(new Async<>());
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
   * Buffers a number of values. It always requests the number of values from the publisher that
   * equals the buffer <code>size</code>.
   *
   * @param size the buffer size.
   * @return the new stream.
   * @since 1.7
   */
  public Chain<T> buffer(final int size) {
    return map(new Buffer<>(size));
  }

  /**
   * Appends a processor that filters objects using the <code>predicate</code> function.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.0
   */
  public Chain<T> filter(final Predicate<T> predicate) {
    return map(new Filter<>(predicate));
  }

  /**
   * Appends a processor that only emits the first value it receives.
   *
   * @return A new chain with the same object type.
   * @since 1.4
   */
  public Chain<T> first() {
    return map(new First<>());
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
   * Appends a processor that only emits the last value it receives.
   *
   * @return A new chain with the same object type.
   * @since 1.4
   */
  public Chain<T> last() {
    return map(new Last<>());
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
   * Appends <code>processor</code> to the chain.
   *
   * @param processor the given processor.
   * @return The new chain.
   * @since 1.5
   */
  public Chain<T> map(final AsyncProcessor<T> processor) {
    publisher.subscribe((Subscriber<? super T>) processor);

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
   * Appends a processor with the mapping function, which transforms the objects. The completion
   * stages are executed in the order of the stream, which completes only after the last stage is
   * completed.
   *
   * @param function the mapping function.
   * @param <R> the object type for the new chain.
   * @return The new chain.
   * @since 1.5.1
   */
  public <R> Chain<R> mapAsync(final Function<T, CompletionStage<R>> function) {
    final Processor<T, CompletionStage<R>> processor = new Mapper<>(function);
    final AsyncProcessor<R> asyncProcessor = new Async<>();

    publisher.subscribe(processor);
    processor.subscribe(asyncProcessor);

    return new Chain<>(asyncProcessor);
  }

  /**
   * Appends a processor that filters objects using negation of the <code>predicate</code> function.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.0
   */
  public Chain<T> notFilter(final Predicate<T> predicate) {
    return map(new NotFilter<>(predicate));
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

  /**
   * Appends a processor that emits values until it receives one that matches <code>predicate</code>
   * , which is also emitted.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.4
   */
  public Chain<T> until(final Predicate<T> predicate) {
    return map(new Until<>(predicate));
  }
}
