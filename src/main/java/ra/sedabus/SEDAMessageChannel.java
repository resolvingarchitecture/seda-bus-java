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
    private final Backpressure backpressure;

    private BlockingQueue<Envelope> queue;
    private File channelDir;

    private final List<MessageConsumer> consumers = new ArrayList<>();
    private final List<MessageChannel> subscriptionChannels = new ArrayList<>();
    private final AtomicInteger roundRobin = new AtomicInteger(0);
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
    /** Most dead letters kept in memory by a channel that has no store directory; the oldest go first. */
    public static final int MAX_IN_MEMORY_DEAD_LETTERS = 1000;
    /** Dead letters of a channel with no store directory (AtMostOnce), newest last. */
    private final java.util.Deque<Envelope> inMemoryDeadLetters = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private final java.util.concurrent.atomic.AtomicLong evictedCount = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deadLetterCount = new java.util.concurrent.atomic.AtomicLong();
    /** First retry delay, doubled for each further attempt (with jitter) up to {@link #retryMaxMs}. */
    private volatile long retryBaseMs = 50;
    private volatile long retryMaxMs = 2_000;
    /**
     * How many times a hand-over to a full or paused next stage is tried before the envelope is dead-lettered. Larger than
     * {@code maxAttempts}, which counts failures of this stage's own consumers: pressure downstream is usually temporary.
     */
    private volatile int handOverAttempts = 10;
    /** Retries waiting for their delay; the channel is not drained while any are pending. */
    private final AtomicInteger pendingRetries = new AtomicInteger();
    private static final java.util.concurrent.ScheduledExecutorService RETRY_TIMER =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "seda-retry");
                t.setDaemon(true);
                return t;
            });
    /** Envelopes this stage has handled but could not yet hand to the next stage. */
    private final java.util.Set<String> awaitingForward = ConcurrentHashMap.newKeySet();

    /**
     * Bounded history of already-delivered ids for ExactlyOnce dedup on replay, oldest first. On a channel with a store
     * directory it is also kept in {@code delivered.log}, so it survives a restart.
     */
    private final Map<String, Boolean> deliveredIds = Collections.synchronizedMap(
            new LinkedHashMap<String, Boolean>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > DEDUP_HISTORY;
                }
            });
    private final Set<String> delivered = Collections.newSetFromMap(deliveredIds);
    /** Lines in delivered.log; when it holds twice the history it is rewritten with the most recent ids. */
    private final java.util.concurrent.atomic.AtomicInteger deliveredLogLines = new java.util.concurrent.atomic.AtomicInteger();
    private static final String DELIVERED_LOG = "delivered.log";

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
        this(bus, name, capacity, dataTypeFilter, serviceLevel, pubSub, maxAttempts, Backpressure.Reject);
    }

    SEDAMessageChannel(MessageBus bus, String name, int capacity, Class dataTypeFilter,
                       ServiceLevel serviceLevel, boolean pubSub, int maxAttempts, Backpressure backpressure) {
        this.bus = bus;
        this.name = name;
        this.capacity = Math.max(1, capacity);
        this.dataTypeFilter = dataTypeFilter;
        this.serviceLevel = serviceLevel == null ? ServiceLevel.AtMostOnce : serviceLevel;
        this.pubSub = pubSub;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.backpressure = backpressure == null ? Backpressure.Reject : backpressure;
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
        awaitingForward.remove(envelope.getId());
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
        if (!admit(e)) {
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

    /**
     * Admit an envelope to {@link #queue} per {@link #backpressure}.
     * {@code queue.offer(e)} alone - the only behaviour this channel had
     * before - is exactly {@link Backpressure#Reject}, kept as the default
     * for every existing constructor so nothing already depending on
     * reject-on-full behaviour changes.
     */
    private boolean admit(Envelope e) {
        switch (backpressure) {
            case DropOldest:
                while (!queue.offer(e)) {
                    Envelope evicted = queue.poll(); // evict oldest to make room; null is a benign race (already drained)
                    if (evicted != null) {
                        evicted(evicted);
                    }
                }
                return true;
            case Block:
                try {
                    queue.put(e);
                    return true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            case Reject:
            case DropNewest:
            default:
                return queue.offer(e);
        }
    }

    /**
     * An envelope pushed out of the queue by {@link Backpressure#DropOldest}: say so, delete its persisted copy (or it
     * would be replayed after a restart as if nothing had happened), forget its retry state, and tell its producer.
     */
    private void evicted(Envelope envelope) {
        evictedCount.incrementAndGet();
        LOG.warning("channel " + name + ": evicted " + envelope.getId() + " to make room (DropOldest); "
                + evictedCount.get() + " evicted so far");
        ack(envelope);
        failedToBus(envelope, "channel " + name + " evicted this envelope to make room (DropOldest)");
    }

    private void failedToBus(Envelope envelope, String reason) {
        if (bus instanceof SEDABus) {
            ((SEDABus) bus).failed(envelope, reason);
        }
    }

    /** How many envelopes {@link Backpressure#DropOldest} has pushed out of this channel since it started. */
    public long evictedCount() {
        return evictedCount.get();
    }

    @Override
    public boolean send(Envelope envelope, Client client) {
        return false; // callbacks are held by the bus, not the channel
    }

    @Override
    public boolean deadLetter(Envelope envelope) {
        deadLetterCount.incrementAndGet();
        if (channelDir == null) {
            // No store directory (AtMostOnce): keep the most recent dead letters in memory, so a failed envelope is not
            // silently discarded. They are lost on restart, which is what AtMostOnce means.
            inMemoryDeadLetters.addLast(envelope);
            while (inMemoryDeadLetters.size() > MAX_IN_MEMORY_DEAD_LETTERS) {
                inMemoryDeadLetters.pollFirst();
            }
            LOG.warning("channel " + name + ": dead-lettered " + envelope.getId() + " (kept in memory; " + inMemoryDeadLetters.size() + " held)");
            return true;
        }
        File dlFile = new File(channelDir, "deadLetter.json");
        try {
            // CREATE as well as APPEND: FileUtil.appendFile only appends, so the first dead letter could never be written.
            java.nio.file.Files.write(dlFile.toPath(),
                    (envelope.toJSON() + System.lineSeparator()).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException ex) {
            LOG.warning(ex.getLocalizedMessage());
            return false;
        }
        return true;
    }

    /** The dead letters held in memory (a channel with no store directory), oldest first. Persisted channels write theirs to deadLetter.json. */
    public java.util.List<Envelope> inMemoryDeadLetters() {
        return new ArrayList<>(inMemoryDeadLetters);
    }

    /** How many envelopes this channel has dead-lettered since it started, wherever they were kept. */
    public long deadLetterCount() {
        return deadLetterCount.get();
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
            failedToBus(envelope, "channel " + name + " dead-lettered this envelope: it has no operation");
            return;
        }

        // An envelope this stage already handled but could not hand on is not handled again: only the hand-over is retried.
        boolean ok;
        if (awaitingForward.contains(envelope.getId())) {
            ok = true;
        } else if (pubSub) {
            ok = fanOut(envelope, op);
        } else {
            ok = deliverPointToPoint(envelope);
        }

        if (ok) {
            // Acknowledge (delete the persisted copy) only once the next stage has accepted the envelope, or the
            // itinerary has ended; until then the persisted copy is what stops a failed hand-over losing it.
            if (bus.completed(envelope)) {
                awaitingForward.remove(envelope.getId());
                markDelivered(envelope);
                ack(envelope);
                return;
            }
            awaitingForward.add(envelope.getId());
            LOG.warning("channel " + name + ": next stage did not accept " + envelope.getId());
        }

        int n = attempts.merge(envelope.getId(), 1, Integer::sum);
        // Failures of this stage's own consumers get maxAttempts; a hand-over that keeps being refused gets a longer budget.
        int budget = awaitingForward.contains(envelope.getId()) ? Math.max(maxAttempts, handOverAttempts) : maxAttempts;
        if (n < budget) {
            long delay = backoffMs(n);
            LOG.fine("channel " + name + ": retry " + n + " for " + envelope.getId() + " in " + delay + " ms");
            scheduleRetry(envelope, delay);
        } else {
            LOG.warning("channel " + name + ": " + envelope.getId() + " failed " + n + " attempts; dead-lettering");
            giveUp(envelope, "channel " + name + " dead-lettered this envelope after " + n + " attempts");
        }
    }

    private void giveUp(Envelope envelope, String reason) {
        deadLetter(envelope);
        ack(envelope);
        failedToBus(envelope, reason);
    }

    /** The delay before retry number {@code attempt}: base times two for each earlier retry, capped, give or take a fifth. */
    long backoffMs(int attempt) {
        long delay = retryBaseMs;
        for (int i = 1; i < attempt && delay < retryMaxMs; i++) {
            delay *= 2;
        }
        delay = Math.min(delay, retryMaxMs);
        double jitter = 0.8 + java.util.concurrent.ThreadLocalRandom.current().nextDouble() * 0.4;
        return Math.max(1, Math.round(delay * jitter));
    }

    /** Puts the envelope back in the queue after {@code delayMs}, without holding a worker thread while it waits. */
    private void scheduleRetry(Envelope envelope, long delayMs) {
        pendingRetries.incrementAndGet();
        try {
            RETRY_TIMER.schedule(() -> {
                try {
                    if (queue.offer(envelope)) {
                        if (bus instanceof SEDABus) {
                            ((SEDABus) bus).wake(this);
                        }
                    } else {
                        LOG.warning("channel " + name + ": no room to retry " + envelope.getId() + "; dead-lettering");
                        giveUp(envelope, "channel " + name + " dead-lettered this envelope: no room to retry");
                    }
                } catch (RuntimeException e) {
                    LOG.log(Level.WARNING, "channel " + name + ": retry of " + envelope.getId() + " failed", e);
                } finally {
                    pendingRetries.decrementAndGet();
                }
            }, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            pendingRetries.decrementAndGet();
            giveUp(envelope, "channel " + name + " dead-lettered this envelope: the retry could not be scheduled");
        }
    }

    /** Sets the retry delay: {@code baseMs} for the first retry, doubled for each further one, up to {@code maxMs}. */
    public void setRetryBackoff(long baseMs, long maxMs) {
        this.retryBaseMs = Math.max(1, baseMs);
        this.retryMaxMs = Math.max(this.retryBaseMs, maxMs);
    }

    /** Sets how many times a refused hand-over to the next stage is tried before the envelope is dead-lettered. */
    public void setHandOverAttempts(int attempts) {
        this.handOverAttempts = Math.max(1, attempts);
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
            appendDelivered(envelope.getId());
        }
    }

    /**
     * Records a delivered id in {@code delivered.log}, synchronously, before the persisted envelope is deleted: a crash
     * after this line and before the delete replays the envelope, which is then skipped. Without a store directory
     * (the channel is AtMostOnce) the history is in memory only. A write that fails is logged, and the in-memory
     * history still applies until the next restart.
     */
    private void appendDelivered(String id) {
        if (channelDir == null) {
            return;
        }
        try {
            Files.write(new File(channelDir, DELIVERED_LOG).toPath(), (id + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND,
                    java.nio.file.StandardOpenOption.SYNC);
            if (deliveredLogLines.incrementAndGet() > 2 * DEDUP_HISTORY) {
                compactDeliveredLog(DEDUP_HISTORY);
            }
        } catch (IOException ex) {
            LOG.warning("channel " + name + ": could not record " + id + " as delivered on disk: " + ex.getLocalizedMessage());
        }
    }

    /** Rewrites {@code delivered.log} with the most recent {@code keep} ids (temp file, then atomic move). */
    synchronized void compactDeliveredLog(int keep) {
        if (channelDir == null) {
            return;
        }
        List<String> recent;
        synchronized (deliveredIds) {
            recent = new ArrayList<>(deliveredIds.keySet());
        }
        if (recent.size() > keep) {
            recent = recent.subList(recent.size() - keep, recent.size());
        }
        File target = new File(channelDir, DELIVERED_LOG);
        File tmp = new File(channelDir, DELIVERED_LOG + ".tmp");
        try {
            Files.write(tmp.toPath(), (String.join("\n", recent) + (recent.isEmpty() ? "" : "\n"))
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            deliveredLogLines.set(recent.size());
        } catch (IOException ex) {
            LOG.warning("channel " + name + ": could not compact " + DELIVERED_LOG + ": " + ex.getLocalizedMessage());
            tmp.delete();
        }
    }

    /** Reads {@code delivered.log} back into the history when the channel starts. */
    private void loadDelivered() {
        File log = new File(channelDir, DELIVERED_LOG);
        if (!log.isFile()) {
            return;
        }
        try {
            List<String> lines = Files.readAllLines(log.toPath(), java.nio.charset.StandardCharsets.UTF_8);
            int count = 0;
            for (String line : lines) {
                String id = line.trim();
                if (!id.isEmpty()) {
                    delivered.add(id);
                    count++;
                }
            }
            deliveredLogLines.set(count);
            if (count > DEDUP_HISTORY) {
                compactDeliveredLog(DEDUP_HISTORY);
            }
            LOG.fine("channel " + name + ": loaded " + delivered.size() + " delivered ids");
        } catch (IOException ex) {
            LOG.warning("channel " + name + ": could not read " + DELIVERED_LOG + ": " + ex.getLocalizedMessage());
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
            loadDelivered();
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
        while ((queued() > 0 || pendingRetries.get() > 0) && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return queued() == 0 && pendingRetries.get() == 0;
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
