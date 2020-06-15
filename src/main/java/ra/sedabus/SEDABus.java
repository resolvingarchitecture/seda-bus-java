package ra.sedabus;

import ra.common.Envelope;
import ra.common.LifeCycle;
import ra.common.MessageConsumer;
import ra.common.ServiceLevel;
import ra.common.route.Route;
import ra.util.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Staged Event-Driven Architectural Bus
 */
public class SEDABus implements LifeCycle {

    private static Logger LOG = Logger.getLogger(SEDABus.class.getName());

    private static SEDABus instance;

    private static final Object instanceLock = new Object();
    public static final Object channelLock = new Object();

    private Properties config;
    private Map<String, MessageChannel> namedChannels;
    private WorkerThreadPool pool;

    private boolean running = false;

    private SEDABus(){}

    public boolean publish(Envelope envelope) {
        Route currentRoute = envelope.getDynamicRoutingSlip().getCurrentRoute();
        if(currentRoute==null) {
            LOG.info("No route current in SEDA Bus. Unable to continue.");
            return true;
        }
        String serviceName = currentRoute.getService();
        MessageChannel channel = namedChannels.get(serviceName);
        if(channel==null) {
            LOG.warning("Unable to continue. No channel for service: "+serviceName);
            return false;
        }
        return channel.send(envelope);
    }

    public static SEDABus getInstance(Properties properties) {
        synchronized (instanceLock) {
            if (instance == null) {
                instance = new SEDABus();
                try {
                    instance.setConfig(properties);
                } catch (Exception e) {
                    LOG.severe(e.getLocalizedMessage());
                    return null;
                }
            }
            if(instance.running = false) {
                if(!instance.start(properties)) {
                    LOG.severe("SEDABus start failed.");
                    instance.running = false;
                }
            }
        }
        return instance;
    }

    public void setConfig(Properties properties) {
        try {
            this.config = Config.loadFromClasspath("ra-sedabus.config", properties, false);
        } catch (Exception e) {
            LOG.warning(e.getLocalizedMessage());
            this.config = properties;
        }
    }

    public void registerChannel(String channelName) {
        MessageChannel ch = new MessageChannel(channelName);
        if(!ch.start(config)) {
            LOG.warning("Channel failed to start.");
            return;
        }
        synchronized (channelLock) {
            namedChannels.put(channelName, ch);
        }
    }

    public void registerChannel(String channelName, ServiceLevel serviceLevel) {
        MessageChannel ch = new MessageChannel(channelName, serviceLevel);
        if(!ch.start(config)) {
            LOG.warning("Channel failed to start.");
            return;
        }
        synchronized (channelLock) {
            namedChannels.put(channelName, ch);
        }
    }

    public void registerChannel(String channelName, int maxSize, boolean pubSub, ServiceLevel serviceLevel, Class dataTypeFilter) {
        MessageChannel ch = new MessageChannel(channelName, maxSize, dataTypeFilter, serviceLevel, pubSub);
        if(!ch.start(config)) {
            LOG.warning("Channel failed to start.");
            return;
        }
        synchronized (channelLock) {
            namedChannels.put(channelName, ch);
        }
    }

    public boolean registerConsumer(String channelName, MessageConsumer consumer) {
        MessageChannel ch = namedChannels.get(channelName);
        ch.registerConsumer(consumer);
        return true;
    }

    @Override
    public boolean start(Properties properties) {
        namedChannels = new HashMap<>();
        pool = new WorkerThreadPool(namedChannels, properties);
        Thread d = new Thread(pool);
        d.start();
        return true;
    }

    @Override
    public boolean pause() {
        return false;
    }

    @Override
    public boolean unpause() {
        return false;
    }

    @Override
    public boolean restart() {
        return false;
    }

    @Override
    public boolean shutdown() {
        return false;
    }

    @Override
    public boolean gracefulShutdown() {
        return false;
    }

    public static void main(String[] args) {
        String configLocation = args[0];
        if(configLocation==null || "".equals(configLocation)) {
            LOG.severe("config location required.");
            System.exit(-1);
        }

    }
}
