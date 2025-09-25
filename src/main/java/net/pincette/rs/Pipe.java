package net.pincette.rs;

import static net.pincette.rs.Box.box;

import java.util.concurrent.Flow.Processor;

/**
 * Creates a pipeline of multiple processors.
 *
 * @param <T> the incoming value type of the first processor.
 * @param <R> the outgoing value type of the last processor.
 * @author Werner Donné
 * @since 3.0
 */
public class Pipe<T, R> extends Delegate<T, R> {
  public Pipe(final Processor<T, R> processor) {
    super(processor);
  }

  public static <T, R> Pipe<T, R> pipe(final Processor<T, R> processor) {
    return new Pipe<>(processor);
  }

  public <U> Pipe<T, U> then(final Processor<R, U> processor) {
    return pipe(box(this, processor));
  }
}
