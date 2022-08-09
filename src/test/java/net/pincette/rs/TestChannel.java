package net.pincette.rs;

import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.TestUtil.read;
import static net.pincette.rs.TestUtil.runFileTest;

import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestChannel {
  @Test
  @DisplayName("file1")
  void file1() {
    runFileTest("/file.pdf", passThrough(), (in, out) -> Arrays.equals(read(in), read(out)));
  }

  @Test
  @DisplayName("file2")
  void file2() {
    runFileTest("/lines.txt", passThrough(), (in, out) -> Arrays.equals(read(in), read(out)));
  }
}
