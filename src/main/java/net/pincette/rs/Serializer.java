package net.pincette.rs;

import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.tryToDoRethrow;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This can be used to serialize the execution of mutating operations instead of locking the state
 * within them.
 *
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Serializer {
  static {
    start();
  }

  private static final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
  private static Thread thread;

  private Serializer() {}

  public static void dispatch(final Runnable runnable) {
    queue.add(runnable);
  }

  private static void run() {
    doForever(() -> tryToDoRethrow(() -> queue.take().run()));
  }

  private static synchronized void start() {
    if (thread == null) {
      thread = new Thread(Serializer::run, Serializer.class.getName());
      thread.setDaemon(true);
      thread.start();
    }
  }
}
