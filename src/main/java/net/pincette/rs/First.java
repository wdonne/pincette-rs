package net.pincette.rs;

/**
 * Emits only the first value it receives.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 1.4
 */
public class First<T> extends Until<T> {
  public First() {
    super(v -> true);
  }
}
