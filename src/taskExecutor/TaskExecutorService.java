package taskExecutor;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * taskExecutor.TaskExecutorService is a service for executing tasks with a fixed thread pool.
 * It provides functionality to submit tasks and manage concurrency for task groups.
 */
public class TaskExecutorService implements Main.TaskExecutor {


    public static final int TIMEOUT = 120;
    private final ExecutorService executorService;
    private final Map<UUID, ReentrantLock> taskGroupLocks = new ConcurrentHashMap<>();
    private final Queue<Callable<?>> taskQueue = new ConcurrentLinkedQueue<>();
    private final Queue<Callable<?>> nonBlockingQueue = new ConcurrentLinkedQueue<>();

    /**
     * Constructs a TaskExecutorService with a specified maximum concurrency level.
     *
     * @param maxConcurrency the maximum number of concurrent threads.
     */
    public TaskExecutorService(int maxConcurrency) {
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
    }


    /**
     * Submits a task for execution and returns a Future representing the pending results of the task.
     *
     * @param task the task to be submitted.
     * @param <T>  the type of the task's result.
     * @return a Future representing the pending results of the task.
     */
    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        System.out.println("Task Submitted || TaskGroup = " + task.taskGroup().groupUUID());
        taskQueue.add(task.taskAction());
        // Obtain or create a lock for the task group
        ReentrantLock lock = taskGroupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new ReentrantLock());
        lock.lock();
        try {
            Callable<?> callableTask = taskQueue.poll();
            if (callableTask != null) {
                return executorService.submit((Callable<T>) callableTask);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * Shuts down the executor service gracefully. If the executor service does not terminate within
     * TIMEOUT seconds, it forces a shutdown.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}