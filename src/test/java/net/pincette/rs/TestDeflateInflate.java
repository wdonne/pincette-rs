package net.pincette.rs;

import static net.pincette.io.PathUtil.delete;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.TestUtil.copyResource;
import static net.pincette.rs.TestUtil.read;
import static net.pincette.rs.TestUtil.runFileTest;
import static net.pincette.rs.Util.deflate;
import static net.pincette.rs.Util.inflate;

import java.io.File;
import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestDeflateInflate {
  @Test
  @DisplayName("deflate inflate 1")
  void deflateInflate1() {
    runFileTest(
        "/file.pdf", box(deflate(), inflate()), (in, out) -> Arrays.equals(read(in), read(out)));
  }

  @Test
  @DisplayName("deflate inflate 2")
  void deflateInflate2() {
    runFileTest(
        "/lines.txt", box(deflate(), inflate()), (in, out) -> Arrays.equals(read(in), read(out)));
  }

  @Test
  @DisplayName("deflate inflate 3")
  void deflateInflate3() {
    final File uncompressed = copyResource("/file.pdf");

    try {
      runFileTest(
          "/file.pdf.zz", inflate(), (in, out) -> Arrays.equals(read(uncompressed), read(out)));
    } finally {
      delete(uncompressed.toPath());
    }
  }

  @Test
  @DisplayName("deflate inflate 4")
  void deflateInflate4() {
    final File uncompressed = copyResource("/lines.txt");

    try {
      runFileTest(
          "/lines.txt.zz", inflate(), (in, out) -> Arrays.equals(read(uncompressed), read(out)));
    } finally {
      delete(uncompressed.toPath());
    }
  }
}
