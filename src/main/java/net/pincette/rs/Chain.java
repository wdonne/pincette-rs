package net.pincette.rs;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Chains processors after the initial publisher.
 *
 * @param <T> the object type of the initial publisher.
 * @author Werner Donn\u00e9
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
    return map(After.after(value));
  }

  /**
   * Asks the upstream for more elements if it hasn't received any before the timeout, until the
   * stream completes.
   *
   * @since 3.0
   */
  public Chain<T> askForever(final Duration timeout) {
    return map(AskForever.askForever(timeout));
  }

  /**
   * Appends the result of <code>value</code> to the stream.
   *
   * @param value the function that produces the value to emit. It may not be <code>null</code>.
   * @return the new stream.
   * @since 1.2.1
   */
  public Chain<T> after(final Supplier<T> value) {
    return map(After.after(value));
  }

  /**
   * Puts <code>value</code> before the stream.
   *
   * @param value the value to emit. It may be <code>null</code>.
   * @return the new stream.
   * @since 1.0
   */
  public Chain<T> before(final T value) {
    return map(Before.before(value));
  }

  /**
   * Puts the result of <code>value</code> before the stream.
   *
   * @param value the function that produces the value to emit. It may not be <code>null</code>.
   * @return the new stream.
   * @since 1.2.1
   */
  public Chain<T> before(final Supplier<T> value) {
    return map(Before.before(value));
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
    return map(Buffer.buffer(size));
  }

  /**
   * Buffers a number of values. It always requests the number of values from the publisher that
   * equals the buffer <code>size</code>.
   *
   * @param size the buffer size.
   * @param timeout the time after which the buffer requests a new value, even if it hasn't received
   *     enough elements yet.
   * @return the new stream.
   * @since 3.0.2
   */
  public Chain<T> buffer(final int size, final Duration timeout) {
    return map(Buffer.buffer(size, timeout));
  }

  /**
   * When the down stream requests more messages this indicates all messages it has received were
   * processed correctly. This is a moment to perform a commit with a function that receives the
   * list of uncommitted messages.
   *
   * @param commit the commit function. New messages are only requested when the completion stage
   *     returns <code>true</code>.
   * @return A new chain with the same object type.
   * @since 3.0
   */
  public Chain<T> commit(final Function<List<T>, CompletionStage<Boolean>> commit) {
    return map(Commit.commit(commit));
  }

  /**
   * Appends a processor that filters objects using the <code>predicate</code> function.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.0
   */
  public Chain<T> filter(final Predicate<T> predicate) {
    return map(Filter.filter(predicate));
  }

  /**
   * Appends a processor that only emits the first value it receives.
   *
   * @return A new chain with the same object type.
   * @since 1.4
   */
  public Chain<T> first() {
    return map(First.first());
  }

  /**
   * Appends a processor that emits the elements from the generated publisher individually.
   *
   * @param function the function that generates the publisher.
   * @return A new chain with the same object type.
   * @since 3.0
   */
  public <R> Chain<R> flatMap(final Function<T, Publisher<R>> function) {
    return map(Flatten.flatMap(function));
  }

  /**
   * Appends a processor that emits the elements from the generated list individually.
   *
   * @param function the function that generates the list.
   * @return A new chain with the same object type.
   * @since 3.0
   */
  public <R> Chain<R> flatMapList(final Function<T, List<R>> function) {
    return map(FlattenList.flatMapList(function));
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
   * Appends a processor that doesn't emit the head of a stream, but instead gives it to a function.
   *
   * @param head the function that receives the first value.
   * @param tail the function that receives all other values.
   * @param <R> the object type for the new chain.
   * @return The new chain.
   * @since 3.0
   */
  public <R> Chain<R> headTail(final Consumer<T> head, final Function<T, R> tail) {
    return map(HeadTail.headTail(head, tail));
  }

  /**
   * Appends a processor that only emits the last value it receives.
   *
   * @return A new chain with the same object type.
   * @since 1.4
   */
  public Chain<T> last() {
    return map(Last.last());
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
    return map(Mapper.map(function));
  }

  /**
   * Appends a processor with the mapping function, which transforms the objects. The completion
   * stages are processed in the order of the stream, which completes only after the last stage is
   * completed. This means the functions may start in parallel, but the completions are emitted in
   * the proper order.
   *
   * @param function the mapping function.
   * @param <R> the object type for the new chain.
   * @return The new chain.
   * @since 1.5.1
   */
  public <R> Chain<R> mapAsync(final Function<T, CompletionStage<R>> function) {
    return map(Async.mapAsync(function));
  }

  /**
   * Appends a processor with the mapping function, which transforms the objects. The functions
   * stages are executed in the order of the stream, which completes only after the last stage is
   * completed. A function call will also receive the result of the previous call, which is <code>
   * null</code> for the first call.
   *
   * @param function the mapping function.
   * @param <R> the object type for the new chain.
   * @return The new chain.
   * @since 3.0
   */
  public <R> Chain<R> mapAsync(final BiFunction<T, R, CompletionStage<R>> function) {
    return map(AsyncDepend.mapAsync(function));
  }

  /**
   * Appends a processor that filters objects using negation of the <code>predicate</code> function.
   *
   * @param predicate the predicate function.
   * @return A new chain with the same object type.
   * @since 1.0
   */
  public Chain<T> notFilter(final Predicate<T> predicate) {
    return map(NotFilter.notFilter(predicate));
  }

  /**
   * Buffers a number of values. It always requests the number of values from the publisher that
   * equals the buffer <code>size</code>. It emits the buffered values as a list.
   *
   * @param size the buffer size.
   * @return the new stream.
   * @since 2.0
   */
  public Chain<List<T>> per(final int size) {
    return map(Per.per(size));
  }

  /**
   * Buffers a number of values. It always requests the number of values from the publisher that
   * equals the buffer <code>size</code>. It emits the buffered values as a list.
   *
   * @param size the buffer size.
   * @param timeout the timeout after which the buffer is flushed. It should be positive.
   * @return the new stream.
   * @since 3.0.2
   */
  public Chain<List<T>> per(final int size, final Duration timeout) {
    return map(Per.per(size, timeout));
  }

  /**
   * Buffers a number of values. It always requests the number of values from the publisher that
   * equals the buffer <code>size</code>. It emits the buffered values as a list.
   *
   * @param size the buffer size.
   * @param timeout the timeout after which the buffer is flushed. It should be positive.
   * @param requestTimeout the time after which an additional element is requested, even if the
   *     upstream publisher hasn't sent all requested elements yet. This provides the opportunity to
   *     the publisher to complete properly when it has fewer elements left than the buffer size. It
   *     may be <code>null</code>.
   * @return the new stream.
   * @since 3.0.2
   */
  public Chain<List<T>> per(final int size, final Duration timeout, final Duration requestTimeout) {
    return map(Per.per(size, timeout, requestTimeout));
  }

  /**
   * Puts <code>value</code> between the emitted values.
   *
   * @param value the value to emit between the emitted values. It may be <code>null</code>.
   * @return The new stream.
   * @since 1.0
   */
  public Chain<T> separate(final T value) {
    return map(Separator.separator(value));
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
    return map(Separator.separator(value));
  }

  /**
   * When the upstream or downstream could cause races, this processor serializes everything with a
   * thread and a blocking queue.
   *
   * @since 3.0
   */
  public Chain<T> split() {
    return map(Split.split());
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
    return map(Until.until(predicate));
  }
}
