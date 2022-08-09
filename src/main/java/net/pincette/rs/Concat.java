package net.pincette.rs;

import static java.util.Arrays.asList;
import static net.pincette.rs.Util.empty;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

/**
 * Concatenates multiple publishers of the same type to form one publisher that completes when the
 * last given publisher completes.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Concat<T> implements Publisher<T> {
  private final Chainer chainer = new Chainer();
  private final List<Publisher<T>> publishers;
  private int position;

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
    if (publishers.isEmpty()) {
      final Publisher<T> empty = empty();

      empty.subscribe(subscriber);
    } else {
      publishers.get(0).subscribe(chainer);
      chainer.subscribe(subscriber);
    }
  }

  private class Chainer extends ProcessorBase<T, T> {
    private long requested;

    @Override
    public void cancel() {
      // Don't cancel when a new subscription is taken, because then this dynamic subscription
      // switching is no longer transparent for the publishers.
    }

    @Override
    protected void emit(final long number) {
      requested += number;
      more();
    }

    private void more() {
      if (requested > 0) {
        subscription.request(1);
      }
    }

    @Override
    public void onComplete() {
      if (position < publishers.size() - 1) {
        publishers.get(++position).subscribe(this);
        more();
      } else {
        super.onComplete();
      }
    }

    @Override
    public void onNext(final T item) {
      --requested;
      subscriber.onNext(item);
      more();
    }
  }
}
