package net.pincette.rs;

import static java.util.Comparator.comparing;
import static java.util.UUID.randomUUID;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.TestUtil.initLogging;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.slide;
import static net.pincette.util.StreamUtil.zip;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Flow.Publisher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestSharded {
  @BeforeAll
  static void beforeAll() {
    initLogging();
  }

  private static boolean inSequence(final List<Integer> values) {
    return slide(values.stream(), 2).allMatch(l -> l.get(1) > l.getFirst());
  }

  private static Publisher<Message> publisher(
      final List<Message> values, final int numberOfShards, final int bufferSize) {
    return with(Source.of(values))
        .map(
            new Sharded<>(
                PassThrough::passThrough, numberOfShards, m -> m.id.hashCode(), bufferSize))
        .get();
  }

  private static boolean sameValues(final List<Message> v1, final List<Message> v2) {
    final List<Message> s1 = new ArrayList<>(v1);
    final List<Message> s2 = new ArrayList<>(v2);

    s1.sort(comparing(m -> m.value));
    s2.sort(comparing(m -> m.value));

    return zip(s1.stream(), s2.stream()).allMatch(pair -> pair.first.value == pair.second.value);
  }

  private static void test(
      final int amount, final int numberOfShards, final int bufferSize, final int times) {
    final List<Message> values = values(amount, numberOfShards);

    runTest(
        result -> verifyPartialOrder(result) && sameValues(values, result),
        () -> publisher(values, numberOfShards, bufferSize),
        times,
        true);
  }

  private static List<Message> values(final int amount, final int numberOfShards) {
    final List<String> ids =
        rangeExclusive(0, numberOfShards).map(i -> randomUUID().toString()).toList();
    final Random random = new Random();

    return rangeExclusive(0, amount)
        .map(i -> new Message(ids.get(random.nextInt(numberOfShards)), i))
        .toList();
  }

  private static boolean verifyPartialOrder(final List<Message> values) {
    final Map<String, List<Integer>> collected = new HashMap<>();

    values.forEach(m -> collected.computeIfAbsent(m.id, k -> new ArrayList<>()).add(m.value));

    return collected.values().stream().allMatch(TestSharded::inSequence);
  }

  @Test
  @DisplayName("sharded 1")
  void sharded1() {
    test(1000, 4, 10, 100);
  }

  @Test
  @DisplayName("sharded 2")
  void sharded2() {
    test(10, 100, 1, 1000);
  }

  @Test
  @DisplayName("sharded 3")
  void sharded3() {
    test(100, 2, 10, 1000);
  }

  @Test
  @DisplayName("sharded 4")
  void sharded4() {
    test(100, 1, 1000, 1000);
  }

  @Test
  @DisplayName("sharded 5")
  void sharded5() {
    test(10, 100, 1000, 1000);
  }

  @Test
  @DisplayName("sharded 6")
  void sharded6() {
    test(100000, 10, 1000, 100);
  }

  record Message(String id, int value) {}
}
