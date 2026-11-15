package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PoolStarvationTest {

    private static final int REQUESTS = 3;
    private static final Duration BORROW_TIMEOUT = Duration.ofMinutes(1);

    @Test
    void allRequestsWaitForASecondConnection() throws Exception {
        ConnectionPool pool = newPool(REQUESTS);
        ExecutorService workers = newWorkers();
        try {
            List<Future<Void>> requests = submitRequests(pool, workers, 2);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            PoolStats snapshot = pool.stats();
            while (snapshot.waiting() != REQUESTS && System.nanoTime() - deadline < 0) {
                Thread.sleep(5);
                snapshot = pool.stats();
            }
            assertEquals(new PoolStats(REQUESTS, 0, REQUESTS, 0, REQUESTS), snapshot);
            for (Future<Void> request : requests) {
                assertFalse(request.isDone());
            }
            if ("1".equals(System.getenv("POOL_STARVATION_PAUSE"))) {
                System.out.println("All requests are waiting. Test JVM PID: " + ProcessHandle.current().pid());
                assertThrows(TimeoutException.class, () -> requests.getFirst().get(30, TimeUnit.SECONDS));
            }
        } finally {
            stop(pool, workers);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void everyRequestFinishesAtTheMinimumSafeSize(int connectionsPerRequest) throws Exception {
        int maxSize = REQUESTS * (connectionsPerRequest - 1) + 1;
        ConnectionPool pool = newPool(maxSize);
        ExecutorService workers = newWorkers();
        try {
            List<Future<Void>> requests = submitRequests(pool, workers, connectionsPerRequest);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            for (Future<Void> request : requests) {
                request.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            }
            assertEquals(new PoolStats(maxSize, maxSize, 0, 0, 0), pool.stats());
        } finally {
            stop(pool, workers);
        }
    }

    private static List<Future<Void>> submitRequests(ConnectionPool pool, ExecutorService workers,
                                                      int connectionsPerRequest) {
        CyclicBarrier holdingConnections = new CyclicBarrier(REQUESTS);
        List<Future<Void>> requests = new ArrayList<>();
        for (int i = 0; i < REQUESTS; i++) {
            requests.add(workers.submit(() -> runRequest(pool, holdingConnections, connectionsPerRequest)));
        }
        return requests;
    }

    private static Void runRequest(ConnectionPool pool, CyclicBarrier holdingConnections,
                                   int connectionsPerRequest) throws Exception {
        List<PooledConnection> held = new ArrayList<>();
        try {
            for (int i = 1; i < connectionsPerRequest; i++) {
                held.add(pool.borrow(BORROW_TIMEOUT));
            }
            holdingConnections.await(5, TimeUnit.SECONDS);
            held.add(pool.borrow(BORROW_TIMEOUT));
            return null;
        } finally {
            for (PooledConnection connection : held.reversed()) {
                connection.close();
            }
        }
    }

    private static ConnectionPool newPool(int maxSize) {
        return new ConnectionPoolImpl(new PoolConfig(0, maxSize, Duration.ofMinutes(1), true),
                TestConnection::new);
    }

    private static ExecutorService newWorkers() {
        AtomicInteger nextId = new AtomicInteger();
        return Executors.newFixedThreadPool(REQUESTS,
                worker -> new Thread(worker, "starvation-request-" + nextId.incrementAndGet()));
    }

    private static void stop(ConnectionPool pool, ExecutorService workers) throws InterruptedException {
        try {
            pool.shutdown(Duration.ZERO);
        } finally {
            workers.shutdownNow();
        }
    }

    private static final class TestConnection implements RawConnection {
        @Override
        public void close() {
        }
    }
}
