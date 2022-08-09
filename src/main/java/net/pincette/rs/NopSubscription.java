package net.pincette.rs;

import java.util.concurrent.Flow.Subscription;

/**
 * This is for emitters that can't be controlled in any way. They can't be stopped and they don't
 * support back pressure.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class NopSubscription implements Subscription {
  public static Subscription nopSubscription() {
    return new NopSubscription();
  }

  public void cancel() {
    // Do nothing.
  }

  public void request(long n) {
    // Do nothing.
  }
}
