package net.pincette.rs;

import static java.util.stream.Collectors.toList;
import static net.pincette.rs.Util.asList;
import static net.pincette.rs.Util.iterate;
import static net.pincette.util.StreamUtil.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

class TestUtil {
  static <T> List<T> asListIter(final Publisher<T> publisher) {
    return stream(iterate(publisher).iterator()).collect(toList());
  }

  static <T> void runTest(final List<T> target, final Supplier<Publisher<T>> publisher) {
    assertEquals(target, asList(publisher.get()));
    assertEquals(target, asListIter(publisher.get()));
  }
}
