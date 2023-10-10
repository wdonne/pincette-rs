package net.pincette.rs;

import java.util.concurrent.Flow.Processor;

/**
 * Blocks cancel signals from being propagated to the upstream.
 *
 * @param <T> the value type
 * @author Werner Donné
 * @since 3.3
 */
public class NeverCancel<T> extends PassThrough<T> {
  public static <T> Processor<T, T> neverCancel() {
    return new NeverCancel<>();
  }

  @Override
  protected void cancelling() {
    // Protect upstream from being cancelled.
  }
}
