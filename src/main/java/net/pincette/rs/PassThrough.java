package net.pincette.rs;

/**
 * A processor which just passes through all values.
 *
 * @param <T> the object type of the values.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class PassThrough<T> extends Mapper<T, T> {
  public PassThrough() {
    super(v -> v);
  }
}
