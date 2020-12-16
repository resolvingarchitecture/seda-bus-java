package ra.sedabus;

import ra.common.Client;
import ra.common.Envelope;
import ra.common.Status;
import ra.common.messaging.MessageBus;
import ra.common.messaging.MessageChannel;
import ra.common.messaging.MessageConsumer;
import ra.common.service.ServiceLevel;
import ra.util.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Staged Event-Driven Architectural Bus supporting push-model async messaging
 * and pull-model (polling). To use the push-model, register a MessageConsumer.
 * To use the pull-model, use the returned MessageChannel from registering a
 * channel to poll against.
 */
public class SEDABus implements MessageBus {

    private static Logger LOG = Logger.getLogger(SEDABus.class.getName());

    private static final Object instanceLock = new Object();
    public static final Object channelLock = new Object();

    private Properties config;
    private Map<String, MessageChannel> namedChannels;
    private WorkerThreadPool pool;
    private Status status = Status.Stopped;
    private Map<String, Client> callbacks;

    public SEDABus(){}

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean publish(Envelope envelope) {
        if(status == Status.Running) {
            MessageChannel channel = lookupChannel(envelope);
            if(channel!=null)
                return channel.send(envelope);
        }
        LOG.fine("SEDABus "+status.name());
        return false;
    }

    @Override
    public boolean publish(Envelope envelope, Client client) {
        if(status == Status.Running) {
            MessageChannel channel = lookupChannel(envelope);
            if(channel!=null) {
                callbacks.put(envelope.getId(), client);
                return channel.send(envelope);
            }
        }
        LOG.fine("SEDABus "+status.name());
        return false;
    }

    @Override
    public boolean completed(Envelope e) {
        if((e.getRoute()!=null && e.getRoute().getRouted()) || (e.getDynamicRoutingSlip()!=null && e.getDynamicRoutingSlip().peekAtNextRoute()==null)) {
            // No more routes; check for Client callback
            Client client = callbacks.get(e.getId());
            if (client != null) {
                client.reply(e);
            }
            // remove Client
            callbacks.remove(e.getId());
        } else if(e.getDynamicRoutingSlip()!=null && e.getDynamicRoutingSlip().peekAtNextRoute()!=null) {
            e.getDynamicRoutingSlip().nextRoute(); // ratchet ahead
            MessageChannel channel = lookupChannel(e);
            if(channel!=null) {
                return channel.send(e);
            }
        }
        return true;
    }

    private MessageChannel lookupChannel(Envelope envelope) {
        String serviceName = null;
        if (envelope.getRoute() != null) {
            serviceName = envelope.getRoute().getService();
        } else if (envelope.getDynamicRoutingSlip() != null && envelope.getDynamicRoutingSlip().getCurrentRoute() != null) {
            serviceName = envelope.getDynamicRoutingSlip().getCurrentRoute().getService();
        } else {
            LOG.warning("Unable to find a service name. Deadlettering...");
            return null;
        }
        MessageChannel channel = namedChannels.get(serviceName);
        if (channel == null) {
            LOG.warning("Unable to continue. No channel for service: " + serviceName);
            return null;
        }
        return channel;
    }

    @Override
    public void setConfig(Properties properties) {
        try {
            this.config = Config.loadFromClasspath("ra-sedabus.config", properties, false);
        } catch (Exception e) {
            LOG.warning(e.getLocalizedMessage());
            this.config = properties;
        }
    }

    @Override
    public MessageChannel registerChannel(String channelName) {
        MessageChannel ch = new SEDAMessageChannel(this, channelName);
        if(!ch.start(config)) {
            LOG.warning("Channel failed to start.");
            return null;
        }
        synchronized (channelLock) {
            namedChannels.put(channelName, ch);
        }
        return ch;
    }

    @Override
    public MessageChannel registerChannel(String channelName, ServiceLevel serviceLevel) {
        MessageChannel ch = new SEDAMessageChannel(this, channelName, serviceLevel);
        if(!ch.start(config)) {
            LOG.warning("Channel failed to start.");
            return null;
        }
        synchronized (channelLock) {
            namedChannels.put(channelName, ch);
        }
        return ch;
    }

    @Override
    public MessageChannel registerChannel(String channelName, int maxSize, ServiceLevel serviceLevel, Class dataTypeFilter, boolean pubSub) {
        MessageChannel ch = new SEDAMessageChannel(this, channelName, maxSize, dataTypeFilter, serviceLevel, pubSub);
        if(!ch.start(config)) {
            LOG.warning("Channel failed to start.");
            return null;
        }
        synchronized (channelLock) {
            namedChannels.put(channelName, ch);
        }
        return ch;
    }

    /**
     * Method requires previous Message Channel created with pubSub set to true.
     * @param channelName Name of the channel with pubSub set to true.
     * @param subscriberChannelName Name of the subscriber channel to create.
     * @param maxSize Max size of subscriber channel.
     * @param serviceLevel Service Level of subscriber channel.
     * @param dataTypeFilter Data type filter for subscriber channel.
     * @return MessageChannel subscriber channel
     */
    @Override
    public MessageChannel registerSubscriberChannel(String channelName, String subscriberChannelName, int maxSize, ServiceLevel serviceLevel, Class dataTypeFilter, boolean pubSub) {
        MessageChannel sch = new SEDAMessageChannel(this, subscriberChannelName, maxSize, dataTypeFilter, serviceLevel, pubSub);
        if(!sch.start(config)) {
            LOG.warning("Channel failed to start.");
            return null;
        }
        synchronized (channelLock) {
            MessageChannel ch = namedChannels.get(channelName);
            if(ch!=null && ch.getPubSub()) {
                ch.registerSubscriptionChannel(sch);
            }
        }
        return sch;
    }

    @Override
    public boolean registerAsynchConsumer(String channelName, MessageConsumer consumer) {
        MessageChannel ch = namedChannels.get(channelName);
        ch.registerAsyncConsumer(consumer);
        return true;
    }

    @Override
    public boolean clearUnprocessed() {
        AtomicBoolean success = new AtomicBoolean(true);
        synchronized (channelLock) {
            namedChannels.forEach((name, ch) -> {
                if(!ch.clearUnprocessed())
                    success.set(false);
            });
        }
        return success.get();
    }

    @Override
    public boolean resumeUnprocessed() {
        AtomicBoolean success = new AtomicBoolean(true);
        synchronized (channelLock) {
            namedChannels.forEach((name, ch) -> {
                if(!ch.sendUnprocessed())
                    success.set(false);
            });
        }
        return success.get();
    }

    @Override
    public boolean start(Properties properties) {
        status = Status.Starting;
        setConfig(properties);
        namedChannels = new HashMap<>();
        callbacks = new HashMap<>();
        pool = new WorkerThreadPool(namedChannels, config);
        Thread d = new Thread(pool);
        d.start();
        status = Status.Running;
        return true;
    }

    @Override
    public boolean pause() {
        status = Status.Paused;
        return true;
    }

    @Override
    public boolean unpause() {
        status = Status.Running;
        return true;
    }

    @Override
    public boolean restart() {
        return shutdown() && start(config);
    }

    @Override
    public boolean shutdown() {
        synchronized (channelLock) {
            namedChannels.forEach((name, ch) -> {
                ch.pause();
                ch.setFlush(true);
            });
        }
        pool.shutdown();
        return true;
    }

    @Override
    public boolean gracefulShutdown() {
        return shutdown();
    }

}
