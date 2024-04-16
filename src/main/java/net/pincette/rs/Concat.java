package net.pincette.rs;

import static java.util.Arrays.asList;
import static net.pincette.rs.Chain.with;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

/**
 * Concatenates multiple publishers of the same type to form one publisher that completes when the
 * last given publisher completes.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.0
 */
public class Concat<T> implements Publisher<T> {
  private final List<Publisher<T>> publishers;

  public Concat(final List<Publisher<T>> publishers) {
    this.publishers = publishers;
  }

  public static <T> Publisher<T> of(final List<Publisher<T>> publishers) {
    return new Concat<>(publishers);
  }

  @SafeVarargs
  public static <T> Publisher<T> of(final Publisher<T>... publishers) {
    return new Concat<>(asList(publishers));
  }

  @Override
  public void subscribe(final Subscriber<? super T> subscriber) {
    (publishers.isEmpty() ? Util.<T>empty() : with(Source.of(publishers)).flatMap(p -> p).get())
        .subscribe(subscriber);
  }
}
