package net.pincette.rs;

import java.util.Optional;
import java.util.concurrent.Flow.Processor;
import net.pincette.function.SideEffect;

/**
 * This buffered processor delegates the stateful encoding of the incoming value stream to an
 * encoder. The reactive co-ordination is taken care of.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Encode<T, R> extends Buffered<T, R> {
  private final Encoder<T, R> encoder;

  public Encode(final Encoder<T, R> encoder) {
    super(1);
    this.encoder = encoder;
  }

  public static <T, R> Processor<T, R> encode(final Encoder<T, R> encoder) {
    return new Encode<>(encoder);
  }

  @Override
  protected void last() {
    Optional.of(encoder.complete()).filter(values -> !values.isEmpty()).ifPresent(this::addValues);
  }

  @Override
  protected boolean onNextAction(final T value) {
    return Optional.of(encoder.encode(value))
        .filter(values -> !values.isEmpty())
        .map(
            values ->
                SideEffect.<Boolean>run(
                        () -> {
                          addValues(values);
                          emit();
                        })
                    .andThenGet(() -> true))
        .orElse(false);
  }
}
