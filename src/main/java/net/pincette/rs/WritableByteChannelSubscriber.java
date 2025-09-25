package net.pincette.rs;

import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToDoRethrow;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * Consumes a reactive stream of <code>ByteBuffer</code> and writes it to a channel.
 *
 * @author Werner Donné
 * @since 3.0
 */
public class WritableByteChannelSubscriber implements Subscriber<ByteBuffer> {
  private final WritableByteChannel channel;
  private final boolean closeOnComplete;
  private boolean completed;
  private Subscription subscription;

  /**
   * Creates a subscriber with a channel. The channel is closed on completion.
   *
   * @param channel the given channel.
   */
  public WritableByteChannelSubscriber(final WritableByteChannel channel) {
    this(channel, true);
  }

  /**
   * Creates a subscriber with a channel.
   *
   * @param channel the given channel.
   * @param closeOnComplete when set to <code>true</code> the channel will be closed when the stream
   *     completes.
   */
  public WritableByteChannelSubscriber(
      final WritableByteChannel channel, final boolean closeOnComplete) {
    this.channel = channel;
    this.closeOnComplete = closeOnComplete;
  }

  /**
   * Creates a subscriber with a channel. The channel is closed on completion.
   *
   * @param channel the given channel.
   * @return The subscriber.
   */
  public static Subscriber<ByteBuffer> writableByteChannel(final WritableByteChannel channel) {
    return new WritableByteChannelSubscriber(channel);
  }

  /**
   * Creates a subscriber with a channel.
   *
   * @param channel the given channel.
   * @param closeOnComplete when set to <code>true</code> the channel will be closed when the stream
   *     completes.
   * @return The subscriber.
   */
  public static Subscriber<ByteBuffer> writableByteChannel(
      final WritableByteChannel channel, final boolean closeOnComplete) {
    return new WritableByteChannelSubscriber(channel, closeOnComplete);
  }

  private void more() {
    subscription.request(1);
  }

  public void onComplete() {
    if (!completed && closeOnComplete) {
      tryToDoRethrow(channel::close);
    }

    completed = true;
  }

  public void onError(final Throwable t) {
    rethrow(t);
  }

  public void onNext(final ByteBuffer buffer) {
    tryToDoRethrow(() -> channel.write(buffer));
    more();
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
    more();
  }
}
