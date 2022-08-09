package net.pincette.rs;

import static java.io.File.createTempFile;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.logging.LogManager.getLogManager;
import static java.util.stream.Collectors.toList;
import static net.pincette.io.StreamConnector.copy;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.rs.Util.asList;
import static net.pincette.rs.Util.iterate;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.rs.WritableByteChannelSubscriber.writableByteChannel;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import net.pincette.util.Util.GeneralException;

class TestUtil {
  static {
    tryToDoRethrow(
        () ->
            getLogManager()
                .readConfiguration(TestUtil.class.getResourceAsStream("/logging.properties")));
  }

  static <T> List<T> asListIter(final Publisher<T> publisher) {
    return stream(iterate(publisher).iterator()).collect(toList());
  }

  static File copyResource(final String resource) {
    final File file = tryToGetRethrow(() -> createTempFile("test", "resource")).orElse(null);

    tryToDoRethrow(
        () ->
            copy(
                requireNonNull(TestUtil.class.getResourceAsStream(resource)),
                new FileOutputStream(requireNonNull(file))));

    return file;
  }

  static byte[] read(final File file) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    tryToDoRethrow(() -> copy(new FileInputStream(file), out));

    return out.toByteArray();
  }

  static void runFileTest(
      final String resource,
      final Processor<ByteBuffer, ByteBuffer> processor,
      final BiPredicate<File, File> predicate) {
    final File in = copyResource(resource);
    final File out = tryToGetRethrow(() -> createTempFile("test", "out")).orElse(null);

    if (in != null && out != null) {
      try {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        with(readableByteChannel(open(in.toPath(), READ)))
            .map(processor)
            .get()
            .subscribe(
                Fanout.of(
                    writableByteChannel(open(out.toPath(), WRITE, SYNC)),
                    onComplete(
                        () -> {
                          assertTrue(predicate.test(in, out));
                          future.complete(null);
                        })));

        future.join();
      } catch (Exception e) {
        throw new GeneralException(e);
      } finally {
        in.delete();
        out.delete();
      }
    }
  }

  static <T> void runTest(final List<T> target, final Supplier<Publisher<T>> publisher) {
    runTest(target, publisher, 1000);
  }

  static <T> void runTest(
      final List<T> target, final Supplier<Publisher<T>> publisher, final int times) {
    for (int i = 0; i < times; ++i) {
      assertEquals(target, new ArrayList<>(asList(publisher.get())));
      assertEquals(target, new ArrayList<>(asListIter(publisher.get())));
    }
  }

  static List<Integer> values(final int from, final int toExclusive) {
    final List<Integer> result = new ArrayList<>(toExclusive - from);

    for (int i = from; i < toExclusive; ++i) {
      result.add(i);
    }

    return result;
  }
}
