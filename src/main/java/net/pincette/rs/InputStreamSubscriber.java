package net.pincette.rs;

import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static java.util.Optional.empty;
import static java.util.concurrent.locks.LockSupport.unpark;
import static net.pincette.rs.Util.parking;
import static net.pincette.util.Pair.pair;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import net.pincette.util.Util.GeneralException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * This is an input stream that can be used as a reactive streams subscriber. It complies with the
 * reactive backpressure mechanism.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class InputStreamSubscriber extends InputStream implements Subscriber<ByteBuffer> {
  private final long timeout;
  private final Thread thread = currentThread();
  private ByteBuffer buffer;
  private boolean ended;
  private Throwable exception;
  private Subscription subscription;

  /** Creates the stream with a timeout of 5 seconds. */
  public InputStreamSubscriber() {
    this(5000);
  }

  /**
   * Creates the stream with a timeout. The value <code>-1</code> indicates no timeout.
   *
   * @param timeout the timeout in millis.
   */
  public InputStreamSubscriber(final long timeout) {
    this.timeout = timeout;
  }

  private Optional<ByteBuffer> getBuffer() {
    if (noData()) {
      readBuffer();
    }

    return ended ? empty() : Optional.of(buffer);
  }

  private boolean noData() {
    return subscription == null || buffer == null || !buffer.hasRemaining();
  }

  public void onComplete() {
    ended = true;
    unpark(thread);
  }

  public void onError(final Throwable t) {
    ended = true;
    exception = t;
    unpark(thread);
    throw new GeneralException(t);
  }

  public void onNext(final ByteBuffer buffer) {
    this.buffer = buffer;
    unpark(thread);
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;

    if (subscription != null) {
      subscription.request(1);
    }
  }

  private void park() {
    while (noData()) {
      parking(this, timeout);
    }
  }

  @Override
  public int read() throws IOException {
    final byte[] b = new byte[1];

    return read(b, 0, b.length) == -1 ? -1 : (255 & b[0]);
  }

  @Override
  public int read(final byte[] bytes, final int offset, final int length) throws IOException {
    if (exception != null) {
      throw new IOException(exception);
    }

    return ended ? -1 : readFromBuffer(bytes, offset, length);
  }

  private void readBuffer() {
    if (subscription != null) {
      subscription.request(1);
    }

    park();
  }

  private int readFromBuffer(final byte[] bytes, final int offset, final int length) {
    return getBuffer()
        .map(b -> pair(b, min(min(length, bytes.length - offset), b.remaining())))
        .map(pair -> pair(pair.first.get(bytes, offset, pair.second), pair.second))
        .map(pair -> pair.second)
        .orElse(-1);
  }
}
