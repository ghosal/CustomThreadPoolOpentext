package taskExecutor;

import java.util.*;
import java.util.concurrent.*;

/**
 * taskExecutor.TaskExecutorService is a service for executing tasks with a fixed thread pool.
 * It provides functionality to submit tasks and manage concurrency for task groups.
 */
public class TaskExecutorService implements Main.TaskExecutor {


    private final Map<UUID, ExecutorService> executorPerTaskGroup = new ConcurrentHashMap<>();
    private final Queue<Callable<?>> taskQueue = new ConcurrentLinkedQueue<>();

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
        ExecutorService executorService = executorPerTaskGroup.computeIfAbsent(task.taskGroup().groupUUID(), k -> Executors.newSingleThreadExecutor());
        try {
            Callable<?> callableTask = taskQueue.poll();
            if (callableTask != null) {
                return executorService.submit((Callable<T>) callableTask);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Gracefully shuts down the ExecutorServices, waiting for the completion of tasks.
     */
    public void shutdownExecutors() {
        if (taskQueue.isEmpty())
            executorPerTaskGroup.forEach((taskGroupId, executorService) -> {
                System.out.println("Shutting down Executor for TaskGroup: " + taskGroupId);
                executorService.shutdown();
            });
    }
}