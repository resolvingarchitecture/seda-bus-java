package ra.sedabus;

import ra.common.Client;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.messaging.MessageBus;
import ra.common.messaging.MessageChannel;
import ra.common.messaging.MessageConsumer;
import ra.common.route.SimpleRoute;
import ra.common.service.ServiceLevel;
import ra.util.FileUtil;
import ra.util.SystemSettings;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * A MessageChannel ensures two or more Message Endpoints are able to communicate.
 *
 * When the ServiceLevel is AtMostOnce, an incoming Envelope is added to the channel's in-memory queue and control
 * is immediately returned to the calling Producer. If the machine were to crash while the Envelope was in memory
 * and hadn't yet been delivered to its destination, it would be lost if the Producer didn't maintain a copy. Attempts
 * are made to send to a Consumer. Upon sending to a Consumer, control is immediately returned without requiring an ack
 * and the Envelope is removed from memory.
 *
 * When the ServiceLevel is AtLeastOnce or ExactlyOnce, the channel is considered a Guaranteed Delivery Channel.
 *
 * As a Guaranteed Delivery Channel, the channel uses a built-in data store to persist messages.
 * When the Producer publishes an Envelope, the publish operation does not complete successfully until the Envelope is safely
 * stored in the channel's data store. Subsequently, the message is not deleted from this data store until it is
 * successfully forwarded to and stored in the next data store of the Consumer. In this way, once the sender successfully sends
 * the Envelope, it is always stored on disk on at least one computer until is successfully delivered to and
 * acknowledged by a Consumer. When the service level is AtLeastOnce, the channel will continue to send to a Consumer
 * until it receives an ack stating the Consumer has persisted the Envelope in their store (or processed it).
 * When the service level is ExactlyOnce, the channel performs a two-phase commit with the Consumer to ensure the Envelope
 * is received and that it is only sent once.
 *
 * When a Service Level within an Envelope is provided, it is used instead of the channel's default.
 *
 * When the dataTypeFilter is set, the channel acts as a Datatype Channel whereby incoming Envelope is ignored if not of
 * that datatype. Therefore, all of the messages on a given channel will contain the same type of data.
 * The Producer, knowing what type the data is, will need to select the appropriate channel to send it on.
 * Each Consumer, knowing what channel the data was received on, will know what its type is.
 *
 * When pubSub is set to false, Envelopes are sent point-to-point ensuring that only one Consumer consumes any given message.
 * If the channel has multiple Consumers, only one of them can successfully consume a particular Envelope.
 * If multiple Consumers try to consume a single Envelope, the channel ensures that only one of them succeeds,
 * so the Consumers do not have to coordinate with each other. The channel can still have multiple Consumers
 * to consume multiple Envelopes concurrently, but only a single Consumer consumes any one Envelope.
 *
 * When pubSub is set to true, the channel will split into multiple output channels,
 * one for each Consumer. When an Envelope is published into the channel, the channel
 * delivers a copy of the Envelope to each of the output channels. Each output channel has only one Consumer,
 * which is only allowed to consume an Envelope once. In this way, each Consumer only gets the Envelope once
 * and consumed copies disappear from their channels.
 *
 * When Consumers subscribe to the channel with pubSub set to true, an additional Channel is created for the Consumer.
 *
 */
final class SEDAMessageChannel implements MessageChannel {

    private static final Logger LOG = Logger.getLogger(SEDAMessageChannel.class.getName());

    private Properties config;
    private boolean accepting = false;
    private BlockingQueue<Envelope> queue;

    private MessageBus bus;
    private String name;
    private File channelDir;
    // Capacity until blocking occurs
    private int capacity = 10;
    private Class dataTypeFilter;
    private ServiceLevel serviceLevel = ServiceLevel.AtLeastOnce;
    private Boolean pubSub = false;
    private List<MessageConsumer> consumers;
    private List<MessageChannel> subscriptionChannels;
    private int roundRobin = 0;
    private boolean flush = false;

    // Channel with Defaults - 10 capacity, no data type filter, fire and forget (in-memory only), point-to-point
    // Name must be unique if creating multiple channels otherwise storage will get stomped over if guaranteed delivery used.
    SEDAMessageChannel(MessageBus bus, String name) {
        this.bus = bus;
        this.name = name;
    }

    SEDAMessageChannel(MessageBus bus, String name, ServiceLevel serviceLevel) {
        this.bus = bus;
        this.name = name;
        this.serviceLevel = serviceLevel;
    }

    SEDAMessageChannel(MessageBus bus, String name, int capacity, Class dataTypeFilter, ServiceLevel serviceLevel, Boolean pubSub) {
        this.bus = bus;
        this.name = name;
        this.capacity = capacity;
        this.dataTypeFilter = dataTypeFilter;
        this.serviceLevel = serviceLevel;
        this.pubSub = pubSub;
    }

    BlockingQueue<Envelope> getQueue() {
        return queue;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int queued() {
        if(queue!=null)
            return queue.size();
        return 0;
    }

    @Override
    public boolean getPubSub() {
        return pubSub;
    }

    @Override
    public void registerAsyncConsumer(MessageConsumer consumer) {
        consumers.add(consumer);
    }

    @Override
    public void registerSubscriptionChannel(MessageChannel channel) {
        subscriptionChannels.add(channel);
    }

    @Override
    public List<MessageChannel> getSubscriptionChannels() {
        return subscriptionChannels;
    }

    @Override
    public void ack(Envelope envelope) {
        LOG.info(Thread.currentThread().getName()+": Removing Envelope.id="+envelope.getId()+" from message queue (size="+queue.size()+")");
        if(remove(envelope)) {
            queue.remove(envelope);
        }
        LOG.info(Thread.currentThread().getName()+": Removed Envelope.id="+envelope.getId()+" from message queue (size="+queue.size()+")");
    }

    /**
     * Send message on channel.
     * @param e Envelope
     */
    @Override
    public boolean send(Envelope e) {
        if(accepting) {
            ServiceLevel serviceLevel = e.getServiceLevel() == null ? this.serviceLevel : e.getServiceLevel();
            if (serviceLevel == ServiceLevel.AtMostOnce) {
                try {
                    if(queue.add(e))
                        LOG.info(Thread.currentThread().getName() + ": Envelope.id=" + e.getId() + " added to message queue (size=" + queue.size() + ")");
                } catch (IllegalStateException ex) {
                    String errMsg = Thread.currentThread().getName() + ": Channel at capacity; rejected Envelope.id=" + e.getId();
                    DLC.addErrorMessage(errMsg, e);
                    LOG.warning(errMsg);
                    return false;
                }
            } else {
                // Guaranteed
                try {
                    if(persist(e) && queue.add(e))
                        LOG.info(Thread.currentThread().getName()+": Envelope.id="+e.getId()+" added to message queue (size="+queue.size()+")");
                } catch (IllegalStateException ex) {
                    String errMsg = Thread.currentThread().getName()+": Channel at capacity; rejected Envelope.id="+e.getId();
                    DLC.addErrorMessage(errMsg, e);
                    LOG.warning(errMsg);
                    return false;
                }
            }
        } else {
            String errMsg = Thread.currentThread().getName()+": Not accepting envelopes.";
            DLC.addErrorMessage(errMsg, e);
            LOG.warning(errMsg);
            return false;
        }
        return true;
    }

    @Override
    public boolean send(Envelope envelope, Client client) {
        // Not supported
        return false;
    }

    /**
     * Receive envelope from channel with blocking.
     * Process all registered async Message Consumers if present.
     * Return Envelope in case called by polling Message Consumer.
     * @return Envelope
     */
    @Override
    public Envelope receive() {
        Envelope next = null;
        try {
            LOG.info(Thread.currentThread().getName()+": Requesting envelope from message queue, blocking...");
            next = queue.take();
            LOG.info(Thread.currentThread().getName()+": Got Envelope.id="+next.getId()+" , queue.size="+queue.size());
        } catch (InterruptedException e) {
            // No need to log
        }
        process(next);
        if((next.getRoute()!=null && next.getRoute().getRouted()) || next.getDynamicRoutingSlip().peekAtNextRoute()==null)
            bus.completed(next);
        return next;
    }

    /**
     * Receive envelope from channel with blocking until timeout.
     * Process all registered async Message Consumers if present.
     * Return Envelope in case called by polling Message Consumer.
     * @param timeout in milliseconds
     * @return Envelope
     */
    @Override
    public Envelope receive(int timeout) {
        Envelope next = null;
        try {
            next = queue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // No need to log
        }
        process(next);
        if((next.getRoute()!=null && next.getRoute().getRouted()) || next.getDynamicRoutingSlip().peekAtNextRoute()==null)
            bus.completed(next);
        return next;
    }

    /**
     * Check the channel for an Envelope returning immediately regardless
     * if an Envelope was found or not. Returns null if no Envelope found.
     * Process all registered async Message Consumers if present.
     * Return Envelope in case called by polling Message Consumer.
     * @return Envelope
     */
    @Override
    public Envelope poll() {
        Envelope next = queue.poll();
        process(next);
        return next;
    }

    private void process(Envelope envelope) {
        if(envelope==null) {
            LOG.warning("No Envelope provided. Unable to process.");
            return;
        }
        String op = null;
        if(envelope.getRoute()!=null && envelope.getRoute().getOperation()!=null)
            op = envelope.getRoute().getOperation();
        else if(envelope.getDynamicRoutingSlip()!=null && envelope.getDynamicRoutingSlip().getCurrentRoute()!=null && envelope.getDynamicRoutingSlip().getCurrentRoute().getOperation()!=null)
            op = envelope.getDynamicRoutingSlip().getCurrentRoute().getOperation();
        if(op==null) {
            LOG.warning("No operation provided. Unable to process Envelope.");
            return;
        }
        if (pubSub && subscriptionChannels!=null && subscriptionChannels.size() > 0) {
            Envelope env;
            for (MessageChannel sch : subscriptionChannels) {
                env = Envelope.envelopeFactory(envelope);
                if(env.getRoute() != null) {
                    SimpleRoute sr = new SimpleRoute();
                    sr.setService(sch.getName());
                    sr.setOperation(op);
                    env.setRoute(sr);
                } else if(env.getDynamicRoutingSlip()!=null && env.getDynamicRoutingSlip().getCurrentRoute()!=null)
                    env.getDynamicRoutingSlip().nextRoute(); // ratchet it
                else {
                    LOG.warning("No routes to publish to subscribers.");
                    return;
                }
                if (!sch.send(env)) {
                    LOG.warning("MessageConsumer.receive() failed during pubsub.");
                }
            }
            ack(envelope);
        } else if(consumers!=null && consumers.size() > 0) {
            // Point-to-Point
            if (roundRobin == consumers.size()) roundRobin = 0;
            MessageConsumer c = consumers.get(roundRobin++);
            if (c.receive(envelope)) {
                ack(envelope);
            } else {
                LOG.warning("MessageConsumer.receive() failed during point-to-point.");
            }
        }
    }

    @Override
    public void setFlush(boolean flush) {
        this.flush = flush;
    }

    @Override
    public boolean getFlush() {
        return flush;
    }

    @Override
    public boolean clearUnprocessed() {
        boolean success = true;
        File[] messages = channelDir.listFiles();
        for(File jsonFile : messages) {
            if(!jsonFile.delete())
                success = false;
        }
        return success;
    }

    @Override
    public boolean sendUnprocessed() {
        File[] messages = channelDir.listFiles();
        for(File jsonFile : messages) {
            byte[] body;
            try {
                body = FileUtil.readFile(jsonFile.getAbsolutePath());
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
                continue;
            }
            Envelope e = new Envelope();
            e.fromJSON(new String(body));
            process(e);
        }
        return true;
    }

    @Override
    public boolean start(Properties properties) {
        config = properties;
        String baseLocation;
        File baseLocDir;
        if(properties.contains("ra.sedabus.channel.locationBase")) {
            baseLocation = properties.getProperty("ra.sedabus.channel.locationBase");
            baseLocDir = new File(baseLocation);
        } else {
            try {
                baseLocDir = SystemSettings.getUserAppDataDir(".ra", this.getClass().getName(), true);
                baseLocation = baseLocDir.getAbsolutePath();
            } catch (IOException e) {
                LOG.severe(e.getLocalizedMessage());
                return false;
            }
        }
        if(!baseLocDir.exists() && !baseLocDir.mkdir()) {
            LOG.severe("Unable to start channel due to unable to create base directory: " + baseLocation);
            return false;
        }
        channelDir = new File(baseLocDir, name);
        if(!channelDir.exists() && !channelDir.mkdir()) {
            LOG.severe("Unable to start channel due to unable to create channel directory: " + baseLocation + "/" + name);
            return false;
        }
        queue = new ArrayBlockingQueue<>(capacity);
        consumers = new ArrayList<>();
        subscriptionChannels = new ArrayList<>();
        accepting = true;
        return true;
    }

    @Override
    public boolean pause() {
        accepting = false;
        return true;
    }

    @Override
    public boolean unpause() {
        accepting = true;
        return true;
    }

    @Override
    public boolean restart() {
        return shutdown() && start(config);
    }

    @Override
    public boolean shutdown() {
        accepting = false;
        long begin = new Date().getTime();
        long runningTime = begin;
        long waitMs = 1000;
        long maxWaitMs = 3 * 1000; // only 3 seconds
        // Wait first to attempt to finish responses
        do {
            waitABit(waitMs);
            runningTime += waitMs;
        } while(queue.size() > 0 && runningTime < maxWaitMs);
        return true;
    }

    @Override
    public boolean gracefulShutdown() {
        accepting = false;
        long begin = new Date().getTime();
        long runningTime = begin;
        long waitMs = 3 * 1000; // Wait longer to allow responses to complete
        long maxWaitMs = 30 * 1000; // up to 30 seconds
        // Wait first to attempt to finish responses
        do {
            waitABit(waitMs);
            runningTime += waitMs;
        } while(queue.size() > 0 && runningTime < maxWaitMs);
        return true;
    }

    private void waitABit(long waitTime) {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {}
    }

    private boolean persist(Envelope e) {
        File jsonFile = new File(channelDir, e.getId()+".json");
        try {
            if(!jsonFile.exists() && !jsonFile.createNewFile()) {
                LOG.warning("Unable to create file to persist Envelope with id="+e.getId());
                return false;
            }
        } catch (IOException ioException) {
            LOG.warning(ioException.getLocalizedMessage());
            return false;
        }
        return FileUtil.writeFile(e.toJSON().getBytes(), jsonFile.getAbsolutePath());
    }

    private boolean remove(Envelope e) {
        File jsonFile = new File(channelDir, e.getId()+".json");
        if(jsonFile.exists() && !jsonFile.delete()) {
            LOG.warning("Unable to delete file of Envelope with id="+e.getId());
            return false;
        }
        return true;
    }
}
