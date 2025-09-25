package net.pincette.rs;

import java.util.concurrent.Flow.Processor;

/**
 * Emits only the first value it receives.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 1.4
 */
public class First<T> extends Until<T> {
  public First() {
    super(v -> true);
  }

  public static <T> Processor<T, T> first() {
    return new First<>();
  }
}
