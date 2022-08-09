package net.pincette.rs;

import static java.io.File.createTempFile;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static net.pincette.rs.Base64.base64Decoder;
import static net.pincette.rs.Base64.base64Encoder;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.rs.TestUtil.copyResource;
import static net.pincette.rs.TestUtil.read;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.rs.WritableByteChannelSubscriber.writableByteChannel;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import net.pincette.util.Util.GeneralException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestBase64 {
  private static void run(final int bufferSize, final boolean oneLine) {
    final File encoded = tryToGetRethrow(() -> createTempFile("test", "encoded.txt")).orElse(null);
    final File in = copyResource("/file.pdf");
    final File out = tryToGetRethrow(() -> createTempFile("test", "out.pdf")).orElse(null);

    if (in != null && encoded != null && out != null) {
      try {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        with(readableByteChannel(open(in.toPath(), READ), true, bufferSize))
            .map(base64Encoder(oneLine))
            .get()
            .subscribe(
                Fanout.of(
                    writableByteChannel(open(encoded.toPath(), WRITE, SYNC)),
                    onComplete(
                        () ->
                            with(readableByteChannel(
                                    open(encoded.toPath(), READ), true, bufferSize))
                                .map(base64Decoder())
                                .get()
                                .subscribe(
                                    Fanout.of(
                                        writableByteChannel(open(out.toPath(), WRITE, SYNC)),
                                        onComplete(
                                            () -> {
                                              assertArrayEquals(read(in), read(out));
                                              future.complete(null);
                                            }))))));

        future.join();
      } catch (Exception e) {
        throw new GeneralException(e);
      } finally {
        in.delete();
        encoded.delete();
        out.delete();
      }
    }
  }

  @Test
  @DisplayName("base64 1")
  void base641() {
    run(0xffff, false);
  }

  @Test
  @DisplayName("base64 2")
  void base642() {
    run(10, false);
  }

  @Test
  @DisplayName("base64 3")
  void base643() {
    run(2, false);
  }

  @Test
  @DisplayName("base64 4")
  void base644() {
    run(0xffff, true);
  }

  @Test
  @DisplayName("base64 5")
  void base645() {
    run(10, true);
  }

  @Test
  @DisplayName("base64 6")
  void base646() {
    run(2, true);
  }
}
