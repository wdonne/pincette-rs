package net.pincette.rs;

import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.wrap;
import static java.util.concurrent.locks.LockSupport.unpark;
import static net.pincette.rs.Util.parking;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * This is an output stream that can be used as a reactive streams publisher. It complies with the
 * reactive backpressure mechanism.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class OutputStreamPublisher extends OutputStream implements Publisher<ByteBuffer> {
  private final Thread thread = currentThread();
  private final long timeout;
  private long requested;
  private Subscriber<? super ByteBuffer> subscriber;

  /** Creates the stream with a timeout of 5 seconds. */
  public OutputStreamPublisher() {
    this(5000);
  }

  /**
   * Creates the stream with a timeout. The value <code>-1</code> indicates no timeout.
   *
   * @param timeout the timeout in millis.
   */
  public OutputStreamPublisher(final long timeout) {
    this.timeout = timeout;
  }

  @Override
  public void close() {
    if (subscriber != null) {
      subscriber.onComplete();
    }
  }

  private void park() {
    while (!requested()) {
      parking(this, timeout);
    }
  }

  private boolean requested() {
    return subscriber != null && requested > 0;
  }

  public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
    this.subscriber = subscriber;

    if (subscriber != null) {
      subscriber.onSubscribe(new StreamSubscription());
    }
  }

  @Override
  public void write(final byte[] bytes) {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, final int offset, final int length) {
    if (!requested()) {
      park();
    }

    --requested;
    subscriber.onNext(wrap(bytes, offset, length));
  }

  @Override
  public void write(final int b) {
    write(new byte[] {(byte) b}, 0, 1);
  }

  private class StreamSubscription implements Subscription {
    public void cancel() {
      // Output streams don't cancel.
    }

    public void request(long n) {
      requested += n;
      unpark(thread);
    }
  }
}
