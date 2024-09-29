package taskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Main {

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID   Unique task identifier.
     * @param taskGroup  Task group.
     * @param taskType   Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T>        Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public static void main(String[] args) {
        TaskExecutorService taskExecutorService = new TaskExecutorService(Runtime.getRuntime().availableProcessors());

        Main.TaskGroup group1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup group2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(UUID.randomUUID(), group1, Main.TaskType.READ, () -> {
            Thread.sleep(500);
            return "Task 1 completed";
        });
        Main.Task<String> task2 = new Main.Task<>(UUID.randomUUID(), group1, Main.TaskType.READ, () -> {
            Thread.sleep(1000);
            return "Task 2 completed";
        });
        Main.Task<String> task3 = new Main.Task<>(UUID.randomUUID(), group1, Main.TaskType.READ, () -> {
            Thread.sleep(2000);
            return "Task 3 completed";
        });
        Main.Task<String> task4 = new Main.Task<>(UUID.randomUUID(), group2, Main.TaskType.READ, () -> {
            Thread.sleep(600);
            return "Task 4 completed";
        });

        Main.Task<String> task5 = new Main.Task<>(UUID.randomUUID(), group2, Main.TaskType.WRITE, () -> {
            Thread.sleep(3000);
            return "Task 5 completed";
        });

        List<Future<String>> futureList = new ArrayList<>();
        futureList.add(taskExecutorService.submitTask(task5));
        futureList.add(taskExecutorService.submitTask(task1));
        futureList.add(taskExecutorService.submitTask(task2));
        futureList.add(taskExecutorService.submitTask(task3));
        futureList.add(taskExecutorService.submitTask(task4));

        futureList.parallelStream().forEach(result -> {
            try {
                System.out.println(result.get());
            } catch (InterruptedException | ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        });

        taskExecutorService.shutdown();
    }

}

