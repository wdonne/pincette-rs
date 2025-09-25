package net.pincette.rs;

import static java.nio.ByteBuffer.wrap;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGet;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * Publishes the contents of a channel as <code>ByteBuffer</code>s.
 *
 * @author Werner Donné
 * @since 3.0
 */
public class ReadableByteChannelPublisher implements Publisher<ByteBuffer> {
  private final ReadableByteChannel channel;
  private final boolean closeOnCancel;
  private final int bufferSize;
  private boolean error;
  private Subscriber<? super ByteBuffer> subscriber;

  /**
   * Creates a publisher with a channel. The channel is closed on completion. The buffer size is
   * <code>0xffff</code>.
   *
   * @param channel the given channel.
   */
  public ReadableByteChannelPublisher(final ReadableByteChannel channel) {
    this(channel, true);
  }

  /**
   * Creates a publisher with a channel. The buffer size is <code>0xffff</code>.
   *
   * @param channel the given channel.
   * @param closeOnCancel when set to <code>true</code> the channel will be closed when the
   *     subscription is cancelled or the channel is completely consumed.
   */
  public ReadableByteChannelPublisher(
      final ReadableByteChannel channel, final boolean closeOnCancel) {
    this(channel, closeOnCancel, 0xffff);
  }

  /**
   * Creates a publisher with a channel.
   *
   * @param channel the given channel.
   * @param closeOnCancel when set to <code>true</code> the channel will be closed when the
   *     subscription is cancelled or the channel is completely consumed.
   * @param bufferSize the size of buffers that are published.
   */
  public ReadableByteChannelPublisher(
      final ReadableByteChannel channel, final boolean closeOnCancel, final int bufferSize) {
    this.channel = channel;
    this.closeOnCancel = closeOnCancel;
    this.bufferSize = bufferSize;
  }

  /**
   * Creates a publisher with a channel. The channel is closed on completion. The buffer size is
   * <code>0xffff</code>.
   *
   * @param channel the given channel.
   * @return The publisher.
   */
  public static Publisher<ByteBuffer> readableByteChannel(final ReadableByteChannel channel) {
    return new ReadableByteChannelPublisher(channel);
  }

  /**
   * Creates a publisher with a channel. The buffer size is <code>0xffff</code>.
   *
   * @param channel the given channel.
   * @param closeOnCancel when set to <code>true</code> the channel will be closed when the
   *     subscription is cancelled or the channel is completely consumed.
   * @return The publisher.
   */
  public static Publisher<ByteBuffer> readableByteChannel(
      final ReadableByteChannel channel, final boolean closeOnCancel) {
    return new ReadableByteChannelPublisher(channel, closeOnCancel);
  }

  /**
   * Creates a publisher with a channel.
   *
   * @param channel the given channel.
   * @param closeOnCancel when set to <code>true</code> the channel will be closed when the
   *     subscription is cancelled or the channel is completely consumed.
   * @param bufferSize the size of buffers that are published.
   * @return The publisher.
   */
  public static Publisher<ByteBuffer> readableByteChannel(
      final ReadableByteChannel channel, final boolean closeOnCancel, final int bufferSize) {
    return new ReadableByteChannelPublisher(channel, closeOnCancel, bufferSize);
  }

  public void subscribe(final Subscriber<? super ByteBuffer> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(new Backpressure());
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      close();
    }

    private void close() {
      if (closeOnCancel) {
        tryToDoRethrow(channel::close);
      }
    }

    private int read(final ByteBuffer buffer) {
      return tryToGet(
              () -> channel.read(buffer),
              e -> {
                error = true;
                subscriber.onError(e);

                return -1;
              })
          .orElse(-1);
    }

    public void request(final long number) {
      if (channel.isOpen()) {
        int read = 0;

        for (long i = 0; i < number && read != -1; ++i) {
          final byte[] array = new byte[bufferSize];
          final ByteBuffer buffer = wrap(array);

          read = read(buffer);

          if (read != -1) {
            subscriber.onNext(wrap(array, 0, read));
          }
        }

        if (read == -1) {
          if (!error) {
            subscriber.onComplete();
          }

          close();
        }
      }
    }
  }
}
