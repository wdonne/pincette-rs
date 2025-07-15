package net.pincette.rs;

import static java.lang.Math.abs;
import static java.util.logging.Level.SEVERE;
import static net.pincette.rs.Util.LOGGER;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToDoRethrow;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import net.pincette.function.RunnableWithException;

/**
 * This can be used to serialize the execution of mutating operations instead of locking the state
 * within them.
 *
 * @author Werner Donné
 * @since 3.0
 */
public class Serializer {
  private static final int DEFAULT_POOL_SIZE = 5;

  private static List<LinkedBlockingQueue<Runnable>> pool;

  private Serializer() {}

  /**
   * Enqueues code to run sequentially.
   *
   * @param runnable the code to be run.
   */
  public static void dispatch(final Runnable runnable) {
    dispatchTask(runnable, null);
  }

  /**
   * Enqueues code to run sequentially.
   *
   * @param runnable the code to be run.
   * @param onException a function to handle an exception.
   */
  public static void dispatch(
      final RunnableWithException runnable, final Consumer<Exception> onException) {
    dispatchTask(() -> tryToDo(runnable, onException), null);
  }

  /**
   * Enqueues code to run sequentially.
   *
   * @param runnable the code to be run.
   * @param key used for consistent hashing. All dispatches with the same key will run sequentially.
   * @since 3.9.3
   */
  public static void dispatch(final Runnable runnable, final String key) {
    dispatchTask(runnable, key);
  }

  /**
   * Enqueues code to run sequentially.
   *
   * @param runnable the code to be run.
   * @param onException a function to handle an exception.
   * @param key used for consistent hashing. All dispatches with the same key will run sequentially.
   * @since 3.9.3
   */
  public static void dispatch(
      final RunnableWithException runnable,
      final Consumer<Exception> onException,
      final String key) {
    dispatchTask(() -> tryToDo(runnable, onException), key);
  }

  private static void dispatchTask(final Runnable runnable, final String key) {
    if (pool == null) {
      startPool(DEFAULT_POOL_SIZE);
    }

    tryToDoRethrow(() -> queue(key).put(runnable));
  }

  @SuppressWarnings("java:S2676") // Not true; hash codes can be negative.
  private static BlockingQueue<Runnable> queue(final String key) {
    return pool.get(key != null ? abs(key.hashCode()) % pool.size() : 0);
  }

  private static void run(final BlockingQueue<Runnable> queue) {
    doForever(
        () ->
            tryToDo(
                () -> queue.take().run(),
                e -> LOGGER.log(SEVERE, e, () -> "Exception in Serializer: " + e.getMessage())));
  }

  /**
   * It creates a global thread pool. This can only be called once because of the consistent
   * hashing.
   *
   * @param size the size of the pool.
   * @since 3.9.3
   */
  public static synchronized void startPool(final int size) {
    if (pool != null) {
      throw new IllegalStateException("Pool already started");
    }

    pool = rangeExclusive(0, size).map(i -> new LinkedBlockingQueue<Runnable>()).toList();
    pool.forEach(Serializer::startThread);
  }

  private static void startThread(final BlockingQueue<Runnable> queue) {
    final Thread t = new Thread(() -> run(queue), Serializer.class.getName());

    t.setDaemon(true);
    t.start();
  }
}
