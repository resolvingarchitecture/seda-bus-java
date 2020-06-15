package ra.sedabus;

import ra.util.AppThread;
import ra.util.Wait;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Thread pool for WorkerThreads.
 */
final class WorkerThreadPool extends AppThread {

    private static final Logger LOG = Logger.getLogger(WorkerThreadPool.class.getName());

    public enum Status {Starting, Running, Stopping, Stopped}

    private Status status = Status.Stopped;

    private static final int NUMBER_OF_CORES = Runtime.getRuntime().availableProcessors();

    private ExecutorService pool;
    private Map<String, MessageChannel> namedChannels;
    private Collection<MessageChannel> channels;
    private int poolSize = NUMBER_OF_CORES * 2; // default
    private int maxPoolSize = NUMBER_OF_CORES * 2; // default
    private Properties properties;
    private AtomicBoolean spin = new AtomicBoolean(true);

    WorkerThreadPool(Map<String, MessageChannel> namedChannels, Properties properties) {
        this.namedChannels = namedChannels;
        this.channels = namedChannels.values();
        this.properties = properties;
    }

    WorkerThreadPool(Map<String, MessageChannel> namedChannels, int poolSize, int maxPoolSize, Properties properties) {
        this.namedChannels = namedChannels;
        this.channels = namedChannels.values();
        this.poolSize = poolSize;
        this.maxPoolSize = maxPoolSize;
        this.properties = properties;
    }

    @Override
    public void run() {
        LOG.info("WorkerThreadPool kicked off...");
        startPool();
        status = Status.Stopped;
    }

    private boolean startPool() {
        status = Status.Starting;
        pool = Executors.newFixedThreadPool(maxPoolSize);
        status = Status.Running;
        LOG.info("Thread pool starting...");
        while(spin.get()) {
            Wait.aMs(100);
            synchronized (SEDABus.channelLock) {
                namedChannels.forEach((name, ch) -> {
                    if(ch.getFlush()) {
                        // Flush Channel
                        while(ch.getQueue().size() > 0) {
                            pool.execute(ch::receive);
                        }
                        if(ch.getSubscriptionChannels()!=null && ch.getSubscriptionChannels().size() > 0) {
                            ch.getSubscriptionChannels().forEach(sch -> {
                                while(sch.getQueue().size() > 0) {
                                    pool.execute(sch::receive);
                                }
                            });
                        }
                        ch.setFlush(false);
                    } else if (ch.getQueue().size() > 0) {
                        pool.execute(ch::receive);
                        if(ch.getSubscriptionChannels()!=null && ch.getSubscriptionChannels().size() > 0) {
                            ch.getSubscriptionChannels().forEach(sch -> {
                                if(sch.getQueue().size() > 0) {
                                    pool.execute(sch::receive);
                                }
                            });
                        }
                    }
                });
            }
        }
        return true;
    }

    boolean shutdown() {
        LOG.info("Shutting down...");
        status = Status.Stopping;
        spin.set(false);
        pool.shutdown();
        try {
            if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
                // pool didn't terminate after the first try
                pool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        status = Status.Stopped;
        return true;
    }

    public Status getStatus() {
        return status;
    }
}
