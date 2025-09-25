package net.pincette.rs;

import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.READ;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.rs.TestUtil.copyResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import net.pincette.util.Util.GeneralException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestDivisibleBy {
  private static void checkBuffers(
      final List<ByteBuffer> buffers, final int n, final long totalSize) {
    for (int i = 0; i < buffers.size() - 1; ++i) {
      assertEquals(0, buffers.get(i).remaining() % n);
    }

    assertTrue(
        Optional.of(buffers.getLast().remaining())
            .map(size -> size % n == 0 || size < n)
            .orElse(false));
    assertEquals(totalSize, totalSize(buffers));
  }

  private static void run(final int bufferSize, final int n) {
    final File in = copyResource("/file.pdf");

    if (in != null) {
      try {
        final List<ByteBuffer> buffers = new ArrayList<>();
        final CompletableFuture<Void> future = new CompletableFuture<>();

        with(readableByteChannel(open(in.toPath(), READ), true, bufferSize))
            .map(Util.divisibleBy(n))
            .get()
            .subscribe(
                lambdaSubscriber(
                    buffers::add,
                    () -> {
                      checkBuffers(buffers, n, in.length());
                      future.complete(null);
                    }));

        future.join();
      } catch (Exception e) {
        throw new GeneralException(e);
      } finally {
        in.delete();
      }
    }
  }

  private static int totalSize(final List<ByteBuffer> buffers) {
    return buffers.stream().mapToInt(ByteBuffer::remaining).sum();
  }

  @Test
  @DisplayName("divisibleBy1")
  void divisibleBy1() {
    run(0xffff, 3);
  }

  @Test
  @DisplayName("divisibleBy2")
  void divisibleBy2() {
    run(10, 3);
  }

  @Test
  @DisplayName("divisibleBy3")
  void divisibleBy3() {
    run(2, 3);
  }

  @Test
  @DisplayName("divisibleBy4")
  void divisibleBy4() {
    run(2, 12);
  }
}
