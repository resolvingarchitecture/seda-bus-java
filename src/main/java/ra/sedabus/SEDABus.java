package ra.sedabus;

import ra.common.Client;
import ra.common.Config;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.route.Route;
import ra.common.Status;
import ra.common.messaging.MessageBus;
import ra.common.messaging.MessageChannel;
import ra.common.messaging.MessageConsumer;
import ra.common.service.ServiceLevel;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * A small, broker-less, staged message bus.
 *
 * <p>Work is decomposed into stages ({@link MessageChannel}s) connected by
 * bounded queues. One shared {@link WorkerThreadPool} drains every stage;
 * each stage is capped at its own concurrency so none can monopolise the pool.
 *
 * <p>Push model: register a {@link MessageConsumer} on a channel. Pull model:
 * keep the {@link MessageChannel} returned by {@code registerChannel} and call
 * {@code poll()} / {@code receive()} yourself.
 *
 * <p>Not implemented: SEDA's adaptive controller (runtime re-tuning of per-stage
 * threads from measured latency, automatic load shedding). Every setting here is
 * static configuration.
 */
public class SEDABus implements MessageBus {

    private static final Logger LOG = Logger.getLogger(SEDABus.class.getName());

    private final Object channelLock = new Object();

    private Properties config;
    private final Map<String, MessageChannel> namedChannels = new ConcurrentHashMap<>();
    private final Map<String, Client> callbacks = new ConcurrentHashMap<>();
    private WorkerThreadPool pool;
    private volatile Status status = Status.Stopped;
    /** Waits out retry delays for every channel of this bus, on its own thread; created at start, stopped at shutdown. */
    private volatile java.util.concurrent.ScheduledExecutorService retryTimer;
    private static final java.util.concurrent.atomic.AtomicInteger TIMER_IDS = new java.util.concurrent.atomic.AtomicInteger();
    private volatile DeadLetterListener deadLetterListener;

    /** Defaults for every channel registered on this bus; see the {@code ra.sedabus.retry.*} properties. */
    private volatile long retryBaseMs = 50;
    private volatile long retryMaxMs = 2_000;
    private volatile int handOverAttempts = 10;

    /**
     * Told when a channel gives up on an envelope: it was dead-lettered after its retries, or pushed out by
     * {@link Backpressure#DropOldest}. Called on a bus thread, so it should return quickly. The producer's callback,
     * when there is one, is also released with an error message on the envelope.
     */
    public interface DeadLetterListener {
        void deadLettered(String channel, Envelope envelope, String reason);
    }

    /** Sets (or, with null, clears) the listener told when an envelope is given up on. */
    public void setDeadLetterListener(DeadLetterListener listener) {
        this.deadLetterListener = listener;
    }

    /** The channel registered under {@code name}, with its settings and counters, or null. */
    public SEDAMessageChannel getChannel(String name) {
        MessageChannel channel = namedChannels.get(name);
        return channel instanceof SEDAMessageChannel ? (SEDAMessageChannel) channel : null;
    }

    public SEDABus() {
    }

    public Status getStatus() {
        return status;
    }

    // -- publishing ----------------------------------------------------

    @Override
    public boolean publish(Envelope envelope) {
        if (status != Status.Running) {
            LOG.fine("SEDABus " + status.name() + "; publish rejected");
            return false;
        }
        MessageChannel channel = lookupChannel(envelope);
        if (channel == null) {
            return false;
        }
        boolean queued = channel.send(envelope);
        if (queued) {
            pool.schedule(channel);
        }
        return queued;
    }

    @Override
    public boolean publish(Envelope envelope, Client callback) {
        if (status != Status.Running) {
            return false;
        }
        MessageChannel channel = lookupChannel(envelope);
        if (channel == null) {
            return false;
        }
        callbacks.put(envelope.getId(), callback);
        boolean queued = channel.send(envelope);
        if (queued) {
            pool.schedule(channel);
        } else {
            callbacks.remove(envelope.getId());
        }
        return queued;
    }

    /**
     * Called by a channel when it finishes an envelope. Advances the routing
     * slip if there is one, otherwise fires the producer callback.
     */
    @Override
    public boolean completed(Envelope e) {
        Route upcoming = e.getDynamicRoutingSlip() == null ? null : e.getDynamicRoutingSlip().peekAtNextRoute();
        if (upcoming != null) {
            // Find the next stage before moving the envelope on, so an unknown stage leaves it exactly as it was.
            MessageChannel next = namedChannel(upcoming.getService(), e);
            if (next == null) {
                return false;
            }
            Route previous = e.getRoute();
            e.ratchet();
            boolean queued = next.send(e);
            if (queued) {
                pool.schedule(next);
                return true;
            }
            // The next stage did not take it (full, paused or refused): put the envelope back where it was, so the
            // stage that finished it can try again instead of losing it.
            e.getDynamicRoutingSlip().addRoute(upcoming);
            e.setRoute(previous);
            return false;
        }
        Client client = callbacks.remove(e.getId());
        if (client != null) {
            client.reply(e);
        }
        return true;
    }

    /**
     * Called by a channel when it gives up on an envelope (dead-lettered, or evicted to make room): releases the producer's
     * callback, so the map does not keep it for ever, and tells the producer, with an error message on the envelope.
     */
    void failed(String channel, Envelope e, String reason) {
        DeadLetterListener listener = deadLetterListener;
        if (listener != null) {
            try {
                listener.deadLettered(channel, e, reason);
            } catch (RuntimeException ex) {
                LOG.warning("dead-letter listener threw for " + e.getId() + ": " + ex);
            }
        }
        Client client = callbacks.remove(e.getId());
        if (client != null) {
            DLC.addErrorMessage(reason, e);
            client.reply(e);
        }
    }

    /** Runs {@code task} after {@code delayMs} on this bus's retry timer. Throws if the bus is not running. */
    void scheduleRetry(Runnable task, long delayMs) {
        java.util.concurrent.ScheduledExecutorService timer = retryTimer;
        if (timer == null) {
            throw new java.util.concurrent.RejectedExecutionException("SEDABus " + status.name());
        }
        timer.schedule(task, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /** Called by a channel that has put an envelope back in its queue from another thread, so the pool drains it. */
    void wake(MessageChannel channel) {
        pool.schedule(channel);
    }

    /** How many producer callbacks are waiting for their envelope to finish. */
    int pendingCallbacks() {
        return callbacks.size();
    }

    private MessageChannel namedChannel(String service, Envelope e) {
        if (service == null) {
            LOG.warning("no service/channel name on the next route of envelope " + e.getId());
            return null;
        }
        MessageChannel channel = namedChannels.get(service);
        if (channel == null) {
            LOG.warning("no channel registered for service " + service + " (envelope " + e.getId() + ")");
        }
        return channel;
    }

    private MessageChannel lookupChannel(Envelope e) {
        String service = null;
        if (e.getRoute() != null) {
            service = e.getRoute().getService();
        } else if (e.getDynamicRoutingSlip() != null
                && e.getDynamicRoutingSlip().getCurrentRoute() != null) {
            service = e.getDynamicRoutingSlip().getCurrentRoute().getService();
        }
        if (service == null) {
            LOG.warning("no service/channel name on envelope " + e.getId() + "; dropping");
            return null;
        }
        MessageChannel channel = namedChannels.get(service);
        if (channel == null) {
            LOG.warning("no channel registered for service " + service);
        }
        return channel;
    }

    // -- registration ------------------------------------------------

    @Override
    public void setConfig(Properties properties) {
        try {
            this.config = Config.loadFromClasspath("ra-sedabus.config", properties, false);
        } catch (Exception e) {
            LOG.warning(e.getLocalizedMessage());
            this.config = properties != null ? properties : new Properties();
        }
    }

    @Override
    public MessageChannel registerChannel(String channelName) {
        return register(new SEDAMessageChannel(this, channelName), 1);
    }

    @Override
    public MessageChannel registerChannel(String channelName, ServiceLevel serviceLevel) {
        return register(new SEDAMessageChannel(this, channelName, serviceLevel), 1);
    }

    @Override
    public MessageChannel registerChannel(String channelName, int maxSize, ServiceLevel serviceLevel,
                                          Class dataTypeFilter, boolean pubSub) {
        return register(new SEDAMessageChannel(this, channelName, maxSize, dataTypeFilter, serviceLevel, pubSub), 1);
    }

    /**
     * Register a stage with an explicit concurrency limit (how many envelopes it
     * may process at once).
     */
    public MessageChannel registerChannel(String channelName, int maxSize, ServiceLevel serviceLevel,
                                          Class dataTypeFilter, boolean pubSub, int concurrency) {
        return register(new SEDAMessageChannel(this, channelName, maxSize, dataTypeFilter, serviceLevel, pubSub), concurrency);
    }

    /**
     * Register a stage with an explicit concurrency limit and back-pressure
     * policy (default {@link Backpressure#Reject} everywhere else in this
     * class, matching every previous release's only behaviour).
     */
    public MessageChannel registerChannel(String channelName, int maxSize, ServiceLevel serviceLevel,
                                          Class dataTypeFilter, boolean pubSub, int concurrency,
                                          Backpressure backpressure) {
        return register(new SEDAMessageChannel(this, channelName, maxSize, dataTypeFilter, serviceLevel, pubSub, 3, backpressure), concurrency);
    }

    @Override
    public MessageChannel registerSubscriberChannel(String channelName, String subscriberChannelName, int maxSize,
                                                    ServiceLevel serviceLevel, Class dataTypeFilter, boolean pubSub) {
        MessageChannel sub = register(
                new SEDAMessageChannel(this, subscriberChannelName, maxSize, dataTypeFilter, serviceLevel, pubSub), 1);
        if (sub == null) {
            return null;
        }
        synchronized (channelLock) {
            MessageChannel parent = namedChannels.get(channelName);
            if (parent != null && parent.getPubSub()) {
                parent.registerSubscriptionChannel(sub);
            } else {
                LOG.warning("parent channel " + channelName + " missing or not pubSub");
            }
        }
        return sub;
    }

    private MessageChannel register(SEDAMessageChannel channel, int concurrency) {
        channel.setRetryBackoff(retryBaseMs, retryMaxMs);
        channel.setHandOverAttempts(handOverAttempts);
        if (!channel.start(config)) {
            LOG.warning("channel " + channel.getName() + " failed to start");
            return null;
        }
        synchronized (channelLock) {
            namedChannels.put(channel.getName(), channel);
        }
        if (pool != null) {
            pool.register(channel.getName(), concurrency);
        }
        return channel;
    }

    @Override
    public boolean registerAsynchConsumer(String channelName, MessageConsumer consumer) {
        MessageChannel channel = namedChannels.get(channelName);
        if (channel == null) {
            LOG.warning("no channel " + channelName + " to attach consumer");
            return false;
        }
        channel.registerAsyncConsumer(consumer);
        return true;
    }

    // -- unprocessed / recovery ------------------------------------

    @Override
    public boolean clearUnprocessed() {
        boolean ok = true;
        for (MessageChannel ch : namedChannels.values()) {
            ok = ch.clearUnprocessed() && ok;
        }
        return ok;
    }

    @Override
    public boolean resumeUnprocessed() {
        boolean ok = true;
        for (MessageChannel ch : namedChannels.values()) {
            ok = ch.sendUnprocessed() && ok;
            pool.schedule(ch);
        }
        return ok;
    }

    // -- LifeCycle -------------------------------------------------

    @Override
    public boolean start(Properties properties) {
        status = Status.Starting;
        setConfig(properties);
        retryBaseMs = longProperty("ra.sedabus.retry.baseMs", 50);
        retryMaxMs = longProperty("ra.sedabus.retry.maxMs", 2_000);
        handOverAttempts = (int) longProperty("ra.sedabus.retry.handOverAttempts", 10);
        int timerId = TIMER_IDS.incrementAndGet();
        java.util.concurrent.ScheduledThreadPoolExecutor timer = new java.util.concurrent.ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "seda-retry-" + timerId);
            t.setDaemon(true);
            return t;
        });
        timer.setRemoveOnCancelPolicy(true);
        retryTimer = timer;
        pool = new WorkerThreadPool(config);
        pool.start();
        status = Status.Running;
        LOG.info("SEDABus running");
        return true;
    }

    @Override
    public boolean pause() {
        status = Status.Paused;
        for (MessageChannel ch : namedChannels.values()) {
            ch.pause();
        }
        return true;
    }

    @Override
    public boolean unpause() {
        for (MessageChannel ch : namedChannels.values()) {
            ch.unpause();
        }
        status = Status.Running;
        return true;
    }

    @Override
    public boolean restart() {
        return shutdown() && start(config);
    }

    @Override
    public boolean shutdown() {
        return doShutdown(false);
    }

    @Override
    public boolean gracefulShutdown() {
        return doShutdown(true);
    }

    private boolean doShutdown(boolean graceful) {
        status = Status.Stopping;
        boolean drained = true;
        for (MessageChannel ch : namedChannels.values()) {
            ch.pause();
            drained = (graceful ? ch.gracefulShutdown() : ch.shutdown()) && drained;
        }
        if (pool != null) {
            pool.shutdown();
        }
        // The channels have waited for their pending retries (up to 30 s for a graceful shutdown, 3 s otherwise); any
        // still waiting are cancelled. Persisted envelopes are replayed at the next start, AtMostOnce ones are lost.
        java.util.concurrent.ScheduledExecutorService timer = retryTimer;
        retryTimer = null;
        if (timer != null) {
            timer.shutdownNow();
        }
        status = Status.Stopped;
        return drained;
    }

    private long longProperty(String key, long fallback) {
        String value = config == null ? null : config.getProperty(key);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            LOG.warning("bad " + key + ": " + value);
            return fallback;
        }
    }
}
