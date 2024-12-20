package net.pincette.rs;

import static java.nio.channels.FileChannel.open;
import static java.nio.file.Files.delete;
import static java.nio.file.StandardOpenOption.READ;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.rs.TestUtil.copyResource;
import static net.pincette.util.Util.autoClose;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestLines {
  private static void run(final int bufferSize, final String resource) {
    tryToDoWithRethrow(
        autoClose(() -> copyResource("/" + resource), in -> delete(in.toPath())),
        in -> {
          final CompletableFuture<Void> future = new CompletableFuture<>();
          final List<String> lines = new ArrayList<>(1000);

          with(readableByteChannel(open(in.toPath(), READ), true, bufferSize))
              .map(Util.lines())
              .get()
              .subscribe(
                  lambdaSubscriber(
                      lines::add,
                      () -> {
                        assertEquals(
                            lines, new BufferedReader(new FileReader(in)).lines().toList());
                        future.complete(null);
                      }));

          future.join();
        });
  }

  @Test
  @DisplayName("lines1")
  void lines1() {
    run(0xffff, "lines.txt");
  }

  @Test
  @DisplayName("lines2")
  void lines2() {
    run(10, "lines.txt");
  }

  @Test
  @DisplayName("lines3")
  void lines3() {
    run(2, "lines.txt");
  }

  @Test
  @DisplayName("lines4")
  void lines4() {
    run(0xffff, "large.csv");
  }

  @Test
  @DisplayName("lines5")
  void lines5() {
    run(10, "large.csv");
  }

  @Test
  @DisplayName("lines6")
  void lines6() {
    run(2, "large.csv");
  }
}
