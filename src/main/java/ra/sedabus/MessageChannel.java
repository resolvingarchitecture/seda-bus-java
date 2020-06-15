package ra.sedabus;

import ra.common.*;
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
final class MessageChannel implements MessageProducer, LifeCycle {

    private static final Logger LOG = Logger.getLogger(MessageChannel.class.getName());

    private boolean accepting = false;
    private BlockingQueue<Envelope> queue;

    private String name;
    private File channelDir;
    // Capacity until blocking occurs
    private int capacity = 10;
    private Class dataTypeFilter;
    private ServiceLevel serviceLevel = ServiceLevel.AtLeastOnce;
    private Boolean pubSub = false;
    private List<MessageConsumer> consumers;
    private int roundRobin = 0;

    // Channel with Defaults - 10 capacity, no data type filter, fire and forget (in-memory only), point-to-point
    // Name must be unique if creating multiple channels otherwise storage will get stomped over if guaranteed delivery used.
    MessageChannel(String name) {
        this.name = name;
    }

    MessageChannel(String name, ServiceLevel serviceLevel) {
        this.name = name;
        this.serviceLevel = serviceLevel;
    }

    MessageChannel(String name, int capacity, Class dataTypeFilter, ServiceLevel serviceLevel, Boolean pubSub) {
        this.name = name;
        this.capacity = capacity;
        this.dataTypeFilter = dataTypeFilter;
        this.serviceLevel = serviceLevel;
        this.pubSub = pubSub;
    }

    BlockingQueue<Envelope> getQueue() {
        return queue;
    }

    void registerConsumer(MessageConsumer consumer) {
        consumers.add(consumer);
    }

    void ack(Envelope envelope) {
        LOG.finest(Thread.currentThread().getName()+": Removing Envelope-"+envelope.getId()+"("+envelope+") from message queue (size="+queue.size()+")");
        queue.remove(envelope);
        LOG.finest(Thread.currentThread().getName()+": Removed Envelope-"+envelope.getId()+"("+envelope+") from message queue (size="+queue.size()+")");
    }

    /**
     * Send message on channel.
     * @param e Envelope
     */
    public boolean send(Envelope e) {
        if(accepting) {
            ServiceLevel serviceLevel = e.getServiceLevel() == null ? this.serviceLevel : e.getServiceLevel();
            if (serviceLevel == ServiceLevel.AtMostOnce) {
                try {
                    if(queue.add(e))
                        LOG.finest(Thread.currentThread().getName() + ": Envelope-" + e.getId() + "(" + e + ") added to message queue (size=" + queue.size() + ")");
                } catch (IllegalStateException ex) {
                    String errMsg = Thread.currentThread().getName() + ": Channel at capacity; rejected Envelope-" + e.getId() + "(" + e + ").";
                    DLC.addErrorMessage(errMsg, e);
                    LOG.warning(errMsg);
                    return false;
                }
            } else {
                // Guaranteed
                try {
                    if(persist(e) && queue.add(e))
                        LOG.finest(Thread.currentThread().getName()+": Envelope-"+e.getId()+"("+e+") added to message queue (size="+queue.size()+")");
                } catch (IllegalStateException ex) {
                    String errMsg = Thread.currentThread().getName()+": Channel at capacity; rejected Envelope-"+e.getId()+"("+e+").";
                    DLC.addErrorMessage(errMsg, e);
                    LOG.warning(errMsg);
                    return false;
                }
            }
        } else {
            String errMsg = Thread.currentThread().getName()+": Not accepting envelopes yet.";
            DLC.addErrorMessage(errMsg, e);
            LOG.warning(errMsg);
            return false;
        }
        return true;
    }

    /**
     * Receive envelope from channel with blocking.
     * @return Envelope
     */
    public Envelope receive() {
        Envelope next = null;
        try {
            LOG.info(Thread.currentThread().getName()+": Requesting envelope from message queue, blocking...");
            next = queue.take();
            LOG.info(Thread.currentThread().getName()+": Got Envelope-"+next.getId()+"("+next+") (queue size="+queue.size()+")");
            if(pubSub) {
                Envelope env;
                for(MessageConsumer c : consumers) {
                    env = Envelope.envelopeFactory(next);
                    if(!c.receive(env)) {
                        LOG.warning("MessageConsumer.receive() failed during pubsub.");
                    }
                }
            } else {
                // Point-to-Point
                if(roundRobin == consumers.size()) roundRobin = 0;
                MessageConsumer c = consumers.get(roundRobin++);
                if(!c.receive(next)) {
                    LOG.warning("MessageConsumer.receive() failed during point-to-point.");
                }
            }
        } catch (InterruptedException e) {
            // No need to log
        }
        return next;
    }

    /**
     * Receive envelope from channel with blocking until timeout.
     * @param timeout in milliseconds
     * @return Envelope
     */
    public Envelope receive(int timeout) {
        Envelope next = null;
        try {
            next = queue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // No need to log
        }
        return next;
    }

    /**
     * Check the channel for an Envelope returning immediately regardless
     * if an Envelope was found or not. Returns null if no Envelope found.
     * @return Envelope
     */
    public Envelope poll() {
        return queue.poll();
    }

    public boolean start(Properties properties) {
        String baseLocation;
        File baseLocDir;
        if(properties.contains("ra.sedabus.channel.locationBase")) {
            baseLocation = properties.getProperty("ra.sedabus.channel.locationBase");
            baseLocDir = new File(baseLocation);
        } else {
            try {
                baseLocDir = SystemSettings.getUserAppDataDir(".ra", "sedabus", true);
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
        accepting = true;
        return true;
    }

    public boolean pause() {
        return false;
    }

    public boolean unpause() {
        return false;
    }

    public boolean restart() {
        return false;
    }

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

    boolean forceShutdown() {
        return shutdown();
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

    private Envelope load(Integer id) {
        File jsonFile = new File(channelDir, id+".json");
        if(!jsonFile.exists()) {
            LOG.warning("Attempting to load Envelope from file system failed; file not present: "+id+".json in channel: "+name);
            return null;
        }
        byte[] body;
        try {
            body = FileUtil.readFile(jsonFile.getAbsolutePath());
        } catch (IOException e) {
            LOG.severe(e.getLocalizedMessage());
            return null;
        }
        Envelope e = new Envelope();
        e.fromJSON(new String(body));
        return e;
    }
}
