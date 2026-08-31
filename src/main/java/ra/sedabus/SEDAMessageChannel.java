package ra.sedabus;

import ra.common.Client;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.FileUtil;
import ra.common.SystemSettings;
import ra.common.messaging.MessageBus;
import ra.common.messaging.MessageChannel;
import ra.common.messaging.MessageConsumer;
import ra.common.route.SimpleRoute;
import ra.common.service.ServiceLevel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One stage of the bus: a bounded queue plus its consumers.
 *
 * <p>Delivery semantics come from the {@link ServiceLevel} (the envelope's own
 * level overrides the channel default when present):
 *
 * <ul>
 *   <li><b>AtMostOnce</b> &ndash; the envelope is queued in memory and control
 *       returns to the producer immediately. If the process dies before the
 *       envelope is delivered it is lost.</li>
 *   <li><b>AtLeastOnce</b> &ndash; the envelope is written to the channel's
 *       on-disk store before {@code send} returns and is not removed until a
 *       consumer has acked it. A crash mid-delivery means the envelope is
 *       replayed on {@link #sendUnprocessed()}, so consumers must be
 *       idempotent.</li>
 *   <li><b>ExactlyOnce</b> &ndash; as AtLeastOnce, plus the channel remembers
 *       the ids it has already delivered (bounded) and skips duplicates on
 *       replay. This makes <i>processing</i> effectively once; it is not a
 *       distributed two-phase commit.</li>
 * </ul>
 *
 * <p>A nacked envelope (consumer returns {@code false}) is retried up to
 * {@code maxAttempts} times, then dead-lettered.
 *
 * <p>When {@code pubSub} is true the channel fans each envelope out to a copy
 * per registered subscription channel; otherwise it is delivered point-to-point,
 * round-robin across the channel's own consumers.
 */
final class SEDAMessageChannel implements MessageChannel {

    private static final Logger LOG = Logger.getLogger(SEDAMessageChannel.class.getName());

    private static final int DEDUP_HISTORY = 100_000;

    private final MessageBus bus;
    private final String name;

    private volatile boolean accepting = false;
    private volatile boolean flush = false;

    private final int capacity;
    private final Class dataTypeFilter;
    private final ServiceLevel serviceLevel;
    private final boolean pubSub;
    private final int maxAttempts;

    private BlockingQueue<Envelope> queue;
    private File channelDir;

    private final List<MessageConsumer> consumers = new ArrayList<>();
    private final List<MessageChannel> subscriptionChannels = new ArrayList<>();
    private final AtomicInteger roundRobin = new AtomicInteger(0);
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

    /** Bounded set of already-delivered ids for ExactlyOnce dedup on replay. */
    private final Set<String> delivered = Collections.newSetFromMap(
            Collections.synchronizedMap(new LinkedHashMap<String, Boolean>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > DEDUP_HISTORY;
                }
            }));

    SEDAMessageChannel(MessageBus bus, String name) {
        this(bus, name, 10, null, ServiceLevel.AtMostOnce, false, 1);
    }

    SEDAMessageChannel(MessageBus bus, String name, ServiceLevel serviceLevel) {
        this(bus, name, 10, null, serviceLevel, false, 1);
    }

    SEDAMessageChannel(MessageBus bus, String name, int capacity, Class dataTypeFilter,
                       ServiceLevel serviceLevel, boolean pubSub) {
        this(bus, name, capacity, dataTypeFilter, serviceLevel, pubSub, 3);
    }

    SEDAMessageChannel(MessageBus bus, String name, int capacity, Class dataTypeFilter,
                       ServiceLevel serviceLevel, boolean pubSub, int maxAttempts) {
        this.bus = bus;
        this.name = name;
        this.capacity = Math.max(1, capacity);
        this.dataTypeFilter = dataTypeFilter;
        this.serviceLevel = serviceLevel == null ? ServiceLevel.AtMostOnce : serviceLevel;
        this.pubSub = pubSub;
        this.maxAttempts = Math.max(1, maxAttempts);
    }

    boolean guaranteed() {
        return serviceLevel != ServiceLevel.AtMostOnce;
    }

    // -- MessageChannel --------------------------------------------------

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int queued() {
        return queue == null ? 0 : queue.size();
    }

    @Override
    public boolean getPubSub() {
        return pubSub;
    }

    @Override
    public void registerAsyncConsumer(MessageConsumer consumer) {
        synchronized (consumers) {
            consumers.add(consumer);
        }
    }

    @Override
    public void registerSubscriptionChannel(MessageChannel channel) {
        synchronized (subscriptionChannels) {
            subscriptionChannels.add(channel);
        }
    }

    @Override
    public List<MessageChannel> getSubscriptionChannels() {
        return subscriptionChannels;
    }

    @Override
    public void ack(Envelope envelope) {
        attempts.remove(envelope.getId());
        if (guaranteed()) {
            removePersisted(envelope);
        }
    }

    /**
     * Queue an envelope for this stage. Honours the effective ServiceLevel and,
     * for a datatype channel, the type filter. Returns false if the channel is
     * paused, the type does not match, or the queue is at capacity.
     */
    @Override
    public boolean send(Envelope e) {
        if (!accepting) {
            DLC.addErrorMessage(Thread.currentThread().getName() + ": channel " + name + " not accepting", e);
            return false;
        }
        if (dataTypeFilter != null) {
            Object content = DLC.getContent(e);
            if (content != null && !dataTypeFilter.isAssignableFrom(content.getClass())) {
                LOG.fine("channel " + name + " dropped envelope " + e.getId() + " (type mismatch)");
                return false;
            }
        }
        ServiceLevel level = e.getServiceLevel() == null ? serviceLevel : e.getServiceLevel();
        if (level != ServiceLevel.AtMostOnce && !persist(e)) {
            return false;
        }
        if (!queue.offer(e)) {
            if (level != ServiceLevel.AtMostOnce) {
                removePersisted(e);
            }
            String msg = Thread.currentThread().getName() + ": channel " + name + " at capacity; rejected " + e.getId();
            DLC.addErrorMessage(msg, e);
            LOG.warning(msg);
            return false;
        }
        return true;
    }

    @Override
    public boolean send(Envelope envelope, Client client) {
        return false; // callbacks are held by the bus, not the channel
    }

    @Override
    public boolean deadLetter(Envelope envelope) {
        if (channelDir == null) {
            return false;
        }
        File dlFile = new File(channelDir, "deadLetter.json");
        try {
            FileUtil.appendFile((envelope.toJSON() + System.lineSeparator()).getBytes(), dlFile.getAbsolutePath());
        } catch (IOException ex) {
            LOG.warning(ex.getLocalizedMessage());
            return false;
        }
        return true;
    }

    /** Blocking receive + process. Used by tests / polling consumers. */
    @Override
    public Envelope receive() {
        try {
            Envelope e = queue.take();
            process(e);
            return e;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public Envelope receive(int timeout) {
        try {
            Envelope e = queue.poll(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (e != null) {
                process(e);
            }
            return e;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Non-blocking receive + process. This is what the worker pool calls. */
    @Override
    public Envelope poll() {
        Envelope e = queue.poll();
        if (e != null) {
            process(e);
        }
        return e;
    }

    private void process(Envelope envelope) {
        if (envelope == null) {
            return;
        }
        String op = operationOf(envelope);
        if (op == null) {
            LOG.warning("channel " + name + ": envelope " + envelope.getId() + " has no operation; dead-lettering");
            deadLetter(envelope);
            ack(envelope);
            return;
        }

        boolean ok;
        if (pubSub) {
            ok = fanOut(envelope, op);
        } else {
            ok = deliverPointToPoint(envelope);
        }

        if (ok) {
            markDelivered(envelope);
            ack(envelope);
            bus.completed(envelope);
            return;
        }

        int n = attempts.merge(envelope.getId(), 1, Integer::sum);
        if (n < maxAttempts) {
            LOG.fine("channel " + name + ": retry " + n + " for " + envelope.getId());
            if (!queue.offer(envelope)) {
                LOG.warning("channel " + name + ": no room to retry " + envelope.getId() + "; dead-lettering");
                deadLetter(envelope);
                ack(envelope);
            }
        } else {
            LOG.warning("channel " + name + ": " + envelope.getId() + " failed " + n + " attempts; dead-lettering");
            deadLetter(envelope);
            ack(envelope);
        }
    }

    private boolean fanOut(Envelope envelope, String op) {
        List<MessageChannel> subs;
        synchronized (subscriptionChannels) {
            subs = new ArrayList<>(subscriptionChannels);
        }
        if (subs.isEmpty()) {
            LOG.warning("pubSub channel " + name + " has no subscribers; dead-lettering " + envelope.getId());
            return false;
        }
        boolean all = true;
        for (MessageChannel sch : subs) {
            Envelope copy = Envelope.envelopeFactory(envelope);
            SimpleRoute sr = new SimpleRoute();
            sr.setService(sch.getName());
            sr.setOperation(op);
            copy.setRoute(sr);
            // publish() (not send()) so the subscriber channel is also scheduled
            all = bus.publish(copy) && all;
        }
        return all;
    }

    private boolean deliverPointToPoint(Envelope envelope) {
        List<MessageConsumer> snapshot;
        synchronized (consumers) {
            snapshot = new ArrayList<>(consumers);
        }
        if (snapshot.isEmpty()) {
            LOG.warning("channel " + name + " has no consumers; dead-lettering " + envelope.getId());
            return false;
        }
        int idx = Math.floorMod(roundRobin.getAndIncrement(), snapshot.size());
        try {
            return snapshot.get(idx).receive(envelope);
        } catch (RuntimeException re) {
            LOG.log(Level.WARNING, "consumer on channel " + name + " threw handling " + envelope.getId(), re);
            return false;
        }
    }

    private void markDelivered(Envelope envelope) {
        if (serviceLevel == ServiceLevel.ExactlyOnce
                || envelope.getServiceLevel() == ServiceLevel.ExactlyOnce) {
            delivered.add(envelope.getId());
        }
    }

    private boolean alreadyDelivered(Envelope envelope) {
        return delivered.contains(envelope.getId());
    }

    private String operationOf(Envelope e) {
        if (e.getRoute() != null && e.getRoute().getOperation() != null) {
            return e.getRoute().getOperation();
        }
        if (e.getDynamicRoutingSlip() != null
                && e.getDynamicRoutingSlip().getCurrentRoute() != null) {
            return e.getDynamicRoutingSlip().getCurrentRoute().getOperation();
        }
        return null;
    }

    // -- flush / unprocessed -------------------------------------------

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
        if (channelDir == null) {
            return true;
        }
        File[] files = channelDir.listFiles((d, n) -> n.endsWith(".json") && !n.equals("deadLetter.json"));
        if (files == null) {
            return true;
        }
        boolean ok = true;
        for (File f : files) {
            if (!f.delete()) {
                ok = false;
            }
        }
        return ok;
    }

    @Override
    public boolean sendUnprocessed() {
        if (channelDir == null) {
            return true;
        }
        File[] files = channelDir.listFiles((d, n) -> n.endsWith(".json") && !n.equals("deadLetter.json"));
        if (files == null || files.length == 0) {
            return true;
        }
        Arrays.sort(files); // filenames are time-ordered
        for (File f : files) {
            byte[] body;
            try {
                body = FileUtil.readFile(f.getAbsolutePath());
            } catch (IOException ex) {
                LOG.warning(ex.getLocalizedMessage());
                continue;
            }
            Envelope e = new Envelope();
            e.fromJSON(new String(body));
            if (alreadyDelivered(e)) {
                LOG.fine("channel " + name + ": skipping already-delivered " + e.getId() + " on replay");
                removePersisted(e);
                continue;
            }
            process(e);
        }
        return true;
    }

    // -- LifeCycle ----------------------------------------------------

    @Override
    public boolean start(Properties properties) {
        String base;
        File baseDir;
        if (properties != null && properties.getProperty("ra.sedabus.channel.locationBase") != null) {
            base = properties.getProperty("ra.sedabus.channel.locationBase");
            baseDir = new File(base);
        } else {
            try {
                baseDir = SystemSettings.getUserAppDataDir(".ra", "sedabus", true);
                base = baseDir.getAbsolutePath();
            } catch (IOException ex) {
                LOG.severe(ex.getLocalizedMessage());
                return false;
            }
        }
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            LOG.severe("channel " + name + ": cannot create " + base);
            return false;
        }
        if (guaranteed()) {
            channelDir = new File(baseDir, name);
            if (!channelDir.exists() && !channelDir.mkdirs()) {
                LOG.severe("channel " + name + ": cannot create " + channelDir);
                return false;
            }
        }
        queue = new ArrayBlockingQueue<>(capacity);
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
        return shutdown() && start(null);
    }

    @Override
    public boolean shutdown() {
        accepting = false;
        return drainWithin(3_000L);
    }

    @Override
    public boolean gracefulShutdown() {
        accepting = false;
        return drainWithin(30_000L);
    }

    private boolean drainWithin(long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (queued() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return queued() == 0;
    }

    // -- persistence -------------------------------------------------

    private static final AtomicInteger PERSIST_SEQ = new AtomicInteger(0);

    private boolean persist(Envelope e) {
        if (channelDir == null) {
            return true;
        }
        String fileName = String.format("%019d-%08d-%s.json",
                System.currentTimeMillis(), PERSIST_SEQ.getAndIncrement(), e.getId());
        File target = new File(channelDir, fileName);
        File tmp = new File(channelDir, fileName + ".tmp");
        try {
            if (!FileUtil.writeFile(e.toJSON().getBytes(), tmp.getAbsolutePath())) {
                return false;
            }
            Files.move(tmp.toPath(), target.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException ex) {
            LOG.warning("channel " + name + ": failed to persist " + e.getId() + ": " + ex.getLocalizedMessage());
            tmp.delete();
            return false;
        }
    }

    private void removePersisted(Envelope e) {
        if (channelDir == null) {
            return;
        }
        File[] files = channelDir.listFiles((d, n) -> n.endsWith("-" + e.getId() + ".json"));
        if (files == null) {
            return;
        }
        for (File f : files) {
            if (f.exists() && !f.delete()) {
                LOG.warning("channel " + name + ": could not delete " + f);
            }
        }
    }
}
