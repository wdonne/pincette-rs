package net.pincette.rs;

import static net.pincette.rs.Box.box;
import static net.pincette.rs.Buffer.buffer;

import java.util.Objects;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * Splits processing over multiple shards using consistent hashing. This may be used in cases where
 * partial ordering is acceptable and more parallelism is desired.
 *
 * @param <T> the type of the incoming values.
 * @param <R> the type of the outgoing values.
 * @author Werner Donné
 * @since 3.11.0
 */
public class Sharded<T, R> extends Delegate<T, R> {
  /**
   * Creates a sharded processor. It uses the standard Java <code>hashCode</code> method and a
   * buffer size of 1000.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   */
  public Sharded(final Supplier<Processor<T, R>> processor, final int numberOfShards) {
    this(processor, numberOfShards, Objects::hashCode, 1000);
  }

  /**
   * Creates a sharded processor.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   * @param hashFunction the function that hashes the incoming elements. From the result the modulo
   *     of the number of shards is taken to determine which shard will get the element. The
   *     function must be consistent.
   * @param bufferSize the upstream request size, which is also the amount of elements that may be
   *     buffered in memory.
   */
  public Sharded(
      final Supplier<Processor<T, R>> processor,
      final int numberOfShards,
      final ToIntFunction<T> hashFunction,
      final int bufferSize) {
    super(box(Util.<T, R>sharded(processor, numberOfShards, hashFunction), buffer(bufferSize)));
  }

  /**
   * Creates a sharded processor. It uses the standard Java <code>hashCode</code> method and a
   * buffer size of 1000.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   */
  public static <T, R> Processor<T, R> sharded(
      final Supplier<Processor<T, R>> processor, final int numberOfShards) {
    return new Sharded<>(processor, numberOfShards);
  }

  /**
   * Creates a sharded processor.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   * @param hashFunction the function that hashes the incoming elements. From the result the modulo
   *     of the number of shards is taken to determine which shard will get the element. The
   *     function must be consistent.
   * @param bufferSize the upstream request size, which is also the amount of elements that may be
   *     buffered in memory.
   */
  public static <T, R> Processor<T, R> sharded(
      final Supplier<Processor<T, R>> processor,
      final int numberOfShards,
      final ToIntFunction<T> hashFunction,
      final int bufferSize) {
    return new Sharded<>(processor, numberOfShards, hashFunction, bufferSize);
  }
}
