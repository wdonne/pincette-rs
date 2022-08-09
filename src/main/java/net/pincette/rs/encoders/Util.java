package net.pincette.rs.encoders;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

class Util {
  private Util() {}

  static ByteBuffer updateCrc(final ByteBuffer buffer, final CRC32 crc) {
    final int position = buffer.position();

    crc.update(buffer);

    return buffer.position(position);
  }
}
