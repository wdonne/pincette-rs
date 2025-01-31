package net.pincette.rs;

import static net.pincette.util.Collections.list;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Processor;

/**
 * Creates a sliding window over the received elements. Each emitted value is a list with a number
 * of elements that equals the window size, except for the last window, which may be smaller.
 *
 * @author Werner Donné
 * @since 3.8.0
 */
public class Slider<T> extends Buffered<T, List<T>> {
  private final List<T> buf;
  private final int window;

  public Slider(int window) {
    super(window);
    this.window = window;
    this.buf = new ArrayList<>(window);
  }

  private static <T> void shiftDown(final List<T> list) {
    for (int i = 0; i < list.size() - 1; i++) {
      list.set(i, list.get(i + 1));
    }
  }

  public static <T> Processor<T, List<T>> slider(final int size) {
    return new Slider<>(size);
  }

  private void flush() {
    addValues(list(new ArrayList<>(buf)));
    emit();
  }

  @Override
  protected void last() {
    if (!buf.isEmpty() && buf.size() < window) {
      flush();
    }
  }

  @Override
  protected boolean onNextAction(final T value) {
    if (buf.size() < window) {
      buf.add(value);
    } else {
      shiftDown(buf);
      buf.set(window - 1, value);
    }

    if (buf.size() == window) {
      flush();

      return true;
    }

    return false;
  }
}
