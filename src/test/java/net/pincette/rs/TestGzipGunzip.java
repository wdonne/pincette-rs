package net.pincette.rs;

import static net.pincette.rs.Box.box;
import static net.pincette.rs.TestUtil.copyResource;
import static net.pincette.rs.TestUtil.read;
import static net.pincette.rs.TestUtil.runFileTest;
import static net.pincette.rs.Util.gunzip;
import static net.pincette.rs.Util.gzip;

import java.io.File;
import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestGzipGunzip {
  @Test
  @DisplayName("gzip gunzip 1")
  void gzipGunzip1() {
    runFileTest(
        "/file.pdf", box(gzip(), gunzip()), (in, out) -> Arrays.equals(read(in), read(out)));
  }

  @Test
  @DisplayName("gzip gunzip 2")
  void gzipGunzip2() {
    runFileTest(
        "/lines.txt", box(gzip(), gunzip()), (in, out) -> Arrays.equals(read(in), read(out)));
  }

  @Test
  @DisplayName("gzip gunzip 3")
  void gzipGunzip3() {
    final File uncompressed = copyResource("/file.pdf");

    try {
      runFileTest(
          "/file.pdf.gz", gunzip(), (in, out) -> Arrays.equals(read(uncompressed), read(out)));
    } finally {
      uncompressed.delete();
    }
  }

  @Test
  @DisplayName("gzip gunzip 4")
  void gzipGunzip4() {
    final File uncompressed = copyResource("/lines.txt");

    try {
      runFileTest(
          "/lines.txt.gz", gunzip(), (in, out) -> Arrays.equals(read(uncompressed), read(out)));
    } finally {
      uncompressed.delete();
    }
  }
}
