package ra.sedabus;

import ra.common.messaging.MessageChannel;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * The one worker pool shared by every stage.
 *
 * <p>There is no polling loop. When a producer publishes to a channel the bus
 * calls {@link #schedule(MessageChannel)}; that submits a drain task <i>iff</i>
 * the channel has work and a free concurrency permit. A drain task pulls a
 * batch off the channel, processes it, releases its permit and re-schedules if
 * more work remains. Each channel is capped at its configured concurrency, so
 * no single stage can monopolise the pool.
 */
final class WorkerThreadPool {

    private static final Logger LOG = Logger.getLogger(WorkerThreadPool.class.getName());

    /** Envelopes one drain task handles before releasing its permit. */
    private static final int BATCH = 64;

    private final ExecutorService exec;
    private final ConcurrentHashMap<String, Semaphore> permits = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    WorkerThreadPool(Properties config) {
        int threads = resolveThreads(config);
        AtomicInteger n = new AtomicInteger(0);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "seda-worker-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.exec = Executors.newFixedThreadPool(threads, tf);
        LOG.fine("SEDA worker pool: " + threads + " threads");
    }

    void start() {
        running = true;
    }

    /** Register a stage's concurrency limit (idempotent). */
    void register(String channel, int concurrency) {
        permits.putIfAbsent(channel, new Semaphore(Math.max(1, concurrency)));
    }

    /**
     * Ensure the channel is being drained. Safe to call from producer threads
     * and from drain tasks; cheap when there is nothing to do.
     */
    void schedule(MessageChannel channel) {
        if (!running) {
            return;
        }
        Semaphore sem = permits.computeIfAbsent(channel.getName(), k -> new Semaphore(1));
        while (channel.queued() > 0 && sem.tryAcquire()) {
            try {
                exec.execute(() -> drain(channel, sem));
            } catch (RejectedExecutionException rex) {
                sem.release();
                return;
            }
        }
    }

    private void drain(MessageChannel channel, Semaphore sem) {
        try {
            for (int i = 0; i < BATCH && running; i++) {
                if (channel.poll() == null) {   // poll() also processes the envelope
                    return;
                }
            }
        } catch (RuntimeException re) {
            LOG.warning("drain error on channel " + channel.getName() + ": " + re);
        } finally {
            sem.release();
            if (running) {
                schedule(channel);
            }
        }
    }

    void shutdown() {
        running = false;
        exec.shutdown();
        try {
            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
            }
        } catch (InterruptedException ie) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static int resolveThreads(Properties config) {
        int cores = Runtime.getRuntime().availableProcessors();
        int threads = Math.max(2, cores);
        if (config == null) {
            return threads;
        }
        String max = config.getProperty("ra.sedabus.pool.max");
        if (max != null) {
            if ("Platform".equalsIgnoreCase(max)) {
                threads = Math.max(2, cores * 2);
            } else {
                try {
                    threads = Math.max(1, Integer.parseInt(max.trim()));
                } catch (NumberFormatException nfe) {
                    LOG.warning("bad ra.sedabus.pool.max: " + max);
                }
            }
        }
        return threads;
    }
}
