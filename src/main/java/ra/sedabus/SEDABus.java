package ra.sedabus;

import ra.common.Client;
import ra.common.Config;
import ra.common.Envelope;
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
        boolean moreRoutes = e.getDynamicRoutingSlip() != null
                && e.getDynamicRoutingSlip().peekAtNextRoute() != null;
        if (moreRoutes) {
            e.ratchet();
            MessageChannel next = lookupChannel(e);
            if (next == null) {
                LOG.warning("routing slip points at unknown channel for " + e.getId());
                return false;
            }
            boolean queued = next.send(e);
            if (queued) {
                pool.schedule(next);
            }
            return queued;
        }
        Client client = callbacks.remove(e.getId());
        if (client != null) {
            client.reply(e);
        }
        return true;
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
        status = Status.Stopped;
        return drained;
    }
}
