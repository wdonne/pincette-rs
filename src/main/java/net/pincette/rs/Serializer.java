package net.pincette.rs;

import static java.util.logging.Level.SEVERE;
import static net.pincette.rs.Util.LOGGER;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToDoRethrow;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import net.pincette.function.RunnableWithException;

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

  private static final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(10000);
  private static Thread thread;

  private Serializer() {}

  public static void dispatch(final Runnable runnable) {
    tryToDoRethrow(() -> queue.put(runnable));
  }

  public static void dispatch(
      final RunnableWithException runnable, final Consumer<Exception> onException) {
    tryToDoRethrow(() -> queue.put(() -> tryToDo(runnable, onException)));
  }

  private static void run() {
    doForever(
        () ->
            tryToDo(
                () -> queue.take().run(),
                e -> LOGGER.log(SEVERE, e, () -> "Exception in Serializer: " + e.getMessage())));
  }

  private static synchronized void start() {
    if (thread == null) {
      thread = new Thread(Serializer::run, Serializer.class.getName());
      thread.setDaemon(true);
      thread.start();
    }
  }
}
