package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {

    private final ConnectionFactory factory;
    private final ReentrantLock lock;
    private final Condition idleChanged;
    private final Condition shutdownProgress;
    private final Thread reaper;
    private final long idleTimeoutNanos;

    // Mutable pool state is guarded by lock.
    private final ArrayDeque<IdleConnection> idle = new ArrayDeque<>();
    private final Set<Lease> borrowed = new HashSet<>();
    private final LinkedHashSet<Condition> waiters = new LinkedHashSet<>();
    private PoolConfig config;
    private int creating;
    // Detached connections no longer count toward capacity, but shutdown must await their close.
    private int closing;
    private boolean stopped;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        idleTimeoutNanos = toNanos(config.idleTimeout());
        lock = new ReentrantLock(config.fair());
        idleChanged = lock.newCondition();
        shutdownProgress = lock.newCondition();
        reaper = new Thread(this::reapIdle, "connection-pool-reaper");
        reaper.setDaemon(true);
        reaper.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        long limit = nonNegativeNanos(timeout, "timeout");
        long started = System.nanoTime();
        lock.lock();
        try {
            ensureRunning();
            if (!waiters.isEmpty() || !canBorrow()) {
                awaitTurn(timeout, limit, started);
            }
            if (!idle.isEmpty()) {
                Lease lease = issue(idle.removeFirst().connection());
                signalNextBorrower();
                return lease;
            }
            creating++;
            signalNextBorrower();
        } finally {
            lock.unlock();
        }
        return createReserved();
    }

    private void awaitTurn(Duration timeout, long limit, long started)
            throws InterruptedException, PoolTimeoutException {
        if (limit == 0) {
            throw new PoolTimeoutException(timeout);
        }
        Condition turn = lock.newCondition();
        waiters.add(turn);
        boolean ready = false;
        try {
            while (true) {
                ensureRunning();
                long remaining = limit - (System.nanoTime() - started);
                if (remaining <= 0) {
                    throw new PoolTimeoutException(timeout);
                }
                if (waiters.getFirst() == turn && canBorrow()) {
                    ready = true;
                    return;
                }
                turn.awaitNanos(remaining);
                // A signal can win the race with interruption, leaving the flag set.
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            }
        } finally {
            boolean wasFirst = waiters.getFirst() == turn;
            waiters.remove(turn);
            if (wasFirst && !ready) {
                signalNextBorrower();
            }
            if (waiters.isEmpty() && !idle.isEmpty()) {
                idleChanged.signal();
            }
        }
    }

    private PooledConnection createReserved() throws ConnectionCreationException {
        RawConnection connection;
        try {
            connection = Objects.requireNonNull(factory.create(), "factory result");
        } catch (Exception failure) {
            lock.lock();
            try {
                creating--;
                signalNextBorrower();
                ensureRunning();
            } finally {
                lock.unlock();
            }
            throw new ConnectionCreationException(failure);
        }

        lock.lock();
        try {
            creating--;
            if (!stopped) {
                return issue(connection);
            }
            closing++;
        } finally {
            lock.unlock();
        }
        closeDetached(List.of(connection));
        throw new IllegalStateException("Pool is shut down");
    }

    @Override
    public void resize(int maxSize) {
        List<RawConnection> detached = new ArrayList<>();
        lock.lock();
        try {
            PoolConfig updated = config.withMaxSize(maxSize);
            ensureRunning();
            config = updated;
            while (total() > maxSize && !idle.isEmpty()) {
                detached.add(idle.removeFirst().connection());
            }
            closing += detached.size();
            signalNextBorrower();
        } finally {
            lock.unlock();
        }
        closeDetached(detached);
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try {
            return config;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(total(), idle.size(), borrowed.size(), creating, waiters.size());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        long limit = nonNegativeNanos(grace, "grace");
        List<RawConnection> detached = new ArrayList<>();
        long started;
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            started = System.nanoTime();
            while (!idle.isEmpty()) {
                detached.add(idle.removeFirst().connection());
            }
            if (limit == 0) {
                detachBorrowed(detached);
            }
            closing += detached.size();
            waiters.forEach(Condition::signal);
            idleChanged.signal();
        } finally {
            lock.unlock();
        }
        closeDetached(detached);

        InterruptedException interruption = null;
        List<RawConnection> forced = new ArrayList<>();
        lock.lock();
        try {
            while (!borrowed.isEmpty()) {
                long remaining = limit - (System.nanoTime() - started);
                if (remaining <= 0) {
                    break;
                }
                try {
                    shutdownProgress.awaitNanos(remaining);
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                } catch (InterruptedException e) {
                    interruption = e;
                    break;
                }
            }
            detachBorrowed(forced);
            closing += forced.size();
        } finally {
            lock.unlock();
        }
        closeDetached(forced);

        while (true) {
            try {
                reaper.join();
                break;
            } catch (InterruptedException e) {
                interruption = e;
            }
        }
        lock.lock();
        try {
            while (closing != 0) {
                try {
                    shutdownProgress.await();
                } catch (InterruptedException e) {
                    interruption = e;
                }
            }
        } finally {
            lock.unlock();
        }
        if (interruption != null) {
            throw interruption;
        }
    }

    private void detachBorrowed(List<RawConnection> detached) {
        for (Lease lease : borrowed) {
            detached.add(lease.connection);
        }
        borrowed.clear();
    }

    private void reapIdle() {
        while (true) {
            List<RawConnection> expired = new ArrayList<>();
            lock.lock();
            try {
                while (!stopped && expired.isEmpty()) {
                    if (!waiters.isEmpty() || idle.size() <= config.minIdle()) {
                        idleChanged.awaitUninterruptibly();
                        continue;
                    }
                    long now = System.nanoTime();
                    long remaining = idleTimeoutNanos - (now - idle.getFirst().since());
                    if (remaining > 0) {
                        try {
                            idleChanged.awaitNanos(remaining);
                        } catch (InterruptedException ignored) {
                            // The stopped flag and its signal control the worker's lifetime.
                        }
                        continue;
                    }
                    while (idle.size() > config.minIdle()
                            && now - idle.getFirst().since() >= idleTimeoutNanos) {
                        expired.add(idle.removeFirst().connection());
                    }
                    closing += expired.size();
                }
                if (stopped) {
                    return;
                }
            } finally {
                lock.unlock();
            }
            closeDetached(expired);
        }
    }

    private void release(Lease lease) {
        lock.lock();
        try {
            boolean discard = stopped || total() > config.maxSize();
            if (!borrowed.remove(lease)) {
                return;
            }
            if (discard) {
                closing++;
            } else {
                idle.addLast(new IdleConnection(lease.connection, System.nanoTime()));
                idleChanged.signal();
            }
            if (stopped) {
                shutdownProgress.signal();
            } else {
                signalNextBorrower();
            }
            if (!discard) {
                return;
            }
        } finally {
            lock.unlock();
        }
        closeDetached(List.of(lease.connection));
    }

    private void closeDetached(List<RawConnection> connections) {
        if (connections.isEmpty()) {
            return;
        }
        try {
            connections.forEach(ConnectionPoolImpl::closePhysical);
        } finally {
            lock.lock();
            try {
                closing -= connections.size();
                if (stopped && closing == 0) {
                    shutdownProgress.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private static void closePhysical(RawConnection connection) {
        try {
            connection.close();
        } catch (Exception ignored) {
            // A detached connection is never used again, even if close fails.
        }
    }

    private int total() {
        return idle.size() + borrowed.size() + creating;
    }

    private boolean canBorrow() {
        return !idle.isEmpty() || total() < config.maxSize();
    }

    private void signalNextBorrower() {
        if (!stopped && !waiters.isEmpty() && canBorrow()) {
            waiters.getFirst().signal();
        }
    }

    private Lease issue(RawConnection connection) {
        Lease lease = new Lease(connection);
        borrowed.add(lease);
        return lease;
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Pool is shut down");
        }
    }

    private static long nonNegativeNanos(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return toNanos(duration);
    }

    private static long toNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private record IdleConnection(RawConnection connection, long since) {
    }

    private final class Lease implements PooledConnection {
        private final RawConnection connection;

        private Lease(RawConnection connection) {
            this.connection = connection;
        }

        @Override
        public RawConnection raw() {
            lock.lock();
            try {
                if (!borrowed.contains(this)) {
                    throw new IllegalStateException("Connection is closed");
                }
                return connection;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            release(this);
        }
    }
}
