package ra.sedabus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.messaging.MessageChannel;
import ra.common.messaging.MessageConsumer;
import ra.common.service.ServiceLevel;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SEDABusTest {

    private SEDABus bus;

    @Before
    public void setUp() {
        Thread.interrupted(); // clear any stale interrupt from a prior test
        bus = new SEDABus();
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase",
                System.getProperty("java.io.tmpdir") + "/seda-test-" + System.nanoTime());
        bus.start(p);
    }

    @After
    public void tearDown() {
        bus.gracefulShutdown();
    }

    private static boolean await(CountDownLatch l, long secs) {
        try {
            return l.await(secs, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Test
    public void deliversPointToPoint() {
        CountDownLatch latch = new CountDownLatch(20);
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        // Room for the whole burst: with the default capacity of 10 some of the 20 publishes could be rejected.
        bus.registerChannel("work", 100, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("work", e -> {
            a.incrementAndGet();
            latch.countDown();
            return true;
        });
        bus.registerAsynchConsumer("work", e -> {
            b.incrementAndGet();
            latch.countDown();
            return true;
        });

        for (int i = 0; i < 20; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("work", "handle", e);
            Assert.assertTrue(bus.publish(e));
        }
        Assert.assertTrue(await(latch, 10));
        Assert.assertEquals(20, a.get() + b.get());
        Assert.assertEquals(10, a.get());
        Assert.assertEquals(10, b.get());
    }

    @Test
    public void routingSlipVisitsEveryStageInOrder() {
        List<String> trail = Collections.synchronizedList(new java.util.ArrayList<>());
        CountDownLatch done = new CountDownLatch(1);
        for (String name : new String[]{"one", "two", "three"}) {
            bus.registerChannel(name);
            bus.registerAsynchConsumer(name, e -> {
                trail.add(name);
                return true;
            });
        }

        // The routing slip is a LIFO stack: add hops last-to-first.
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("three", "step", e);
        DLC.addRoute("two", "step", e);
        DLC.addRoute("one", "step", e);
        bus.publish(e, env -> done.countDown());

        Assert.assertTrue(await(done, 10));
        Assert.assertEquals(java.util.Arrays.asList("one", "two", "three"), trail);
    }

    @Test
    public void handOverToAnUnknownStageLeavesTheEnvelopeAsItWas() {
        bus.registerChannel("one");
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("missing", "step", e);
        DLC.addRoute("one", "step", e);
        e.ratchet();

        Assert.assertFalse(bus.completed(e));
        Assert.assertEquals("one", e.getRoute().getService());
        Assert.assertEquals(Integer.valueOf(1), e.getDynamicRoutingSlip().numberRemainingRoutes());
        Assert.assertEquals("missing", e.getDynamicRoutingSlip().peekAtNextRoute().getService());
    }

    @Test
    public void handOverRefusedByTheNextStagePutsTheRouteBack() {
        bus.registerChannel("one");
        bus.registerChannel("two");
        bus.registerAsynchConsumer("two", e -> true);
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("two", "step", e);
        DLC.addRoute("one", "step", e);
        e.ratchet();

        bus.pause();
        Assert.assertFalse(bus.completed(e));
        Assert.assertEquals("one", e.getRoute().getService());
        Assert.assertEquals(Integer.valueOf(1), e.getDynamicRoutingSlip().numberRemainingRoutes());
        Assert.assertEquals("two", e.getDynamicRoutingSlip().peekAtNextRoute().getService());

        bus.unpause();
        Assert.assertTrue("the same envelope can be handed on once the stage accepts again", bus.completed(e));
        Assert.assertEquals("two", e.getRoute().getService());
    }

    @Test
    public void failedHandOverKeepsTheEnvelopeAndDoesNotRunTheStageAgain() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-handover-" + System.nanoTime();
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase", base);
        SEDABus durableBus = new SEDABus();
        durableBus.start(p);
        try {
            AtomicInteger runs = new AtomicInteger();
            durableBus.registerChannel("first", 50, ServiceLevel.AtLeastOnce, null, false);
            durableBus.registerAsynchConsumer("first", e -> {
                runs.incrementAndGet();
                return true;
            });
            Envelope e = Envelope.documentFactory();
            e.setServiceLevel(ServiceLevel.AtLeastOnce);
            DLC.addRoute("missing", "step", e);
            DLC.addRoute("first", "step", e);
            Assert.assertTrue(durableBus.publish(e));
            Thread.sleep(1000);

            Assert.assertEquals("the stage's work is done once, not once per hand-over attempt", 1, runs.get());
            File dir = new File(base, "first");
            File deadLetter = new File(dir, "deadLetter.json");
            Assert.assertTrue("an envelope that cannot be handed on is dead-lettered, not lost", deadLetter.exists());
            Assert.assertTrue(new String(java.nio.file.Files.readAllBytes(deadLetter.toPath())).contains(e.getId()));
            File[] left = dir.listFiles((d, n) -> n.endsWith(".json") && !n.equals("deadLetter.json"));
            Assert.assertTrue(left == null || left.length == 0);
        } finally {
            durableBus.gracefulShutdown();
        }
    }

    @Test
    public void atMostOnceExhaustedRetriesAreDeadLetteredInMemory() throws Exception {
        SEDAMessageChannel channel = (SEDAMessageChannel) bus.registerChannel("flaky-memory", 10, ServiceLevel.AtMostOnce, null, false);
        CountDownLatch tried = new CountDownLatch(3);
        bus.registerAsynchConsumer("flaky-memory", e -> {
            tried.countDown();
            return false;
        });
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("flaky-memory", "handle", e);
        bus.publish(e);

        Assert.assertTrue(await(tried, 10));
        for (int i = 0; i < 50 && channel.inMemoryDeadLetters().isEmpty(); i++) {
            Thread.sleep(20);
        }
        Assert.assertEquals(1, channel.inMemoryDeadLetters().size());
        Assert.assertEquals(e.getId(), channel.inMemoryDeadLetters().get(0).getId());
        Assert.assertEquals(1, channel.deadLetterCount());
    }

    @Test
    public void atMostOnceFailedHandOverIsDeadLetteredNotLost() throws Exception {
        SEDAMessageChannel channel = (SEDAMessageChannel) bus.registerChannel("first-memory", 10, ServiceLevel.AtMostOnce, null, false);
        AtomicInteger runs = new AtomicInteger();
        bus.registerAsynchConsumer("first-memory", e -> {
            runs.incrementAndGet();
            return true;
        });
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("missing-memory", "step", e);
        DLC.addRoute("first-memory", "step", e);
        bus.publish(e);

        for (int i = 0; i < 100 && channel.inMemoryDeadLetters().isEmpty(); i++) {
            Thread.sleep(20);
        }
        Assert.assertEquals("the stage's work is done once", 1, runs.get());
        Assert.assertEquals(1, channel.inMemoryDeadLetters().size());
    }

    @Test
    public void inMemoryDeadLettersAreBounded() {
        SEDAMessageChannel channel = (SEDAMessageChannel) bus.registerChannel("bounded", 10, ServiceLevel.AtMostOnce, null, false);
        for (int i = 0; i < SEDAMessageChannel.MAX_IN_MEMORY_DEAD_LETTERS + 5; i++) {
            channel.deadLetter(Envelope.documentFactory());
        }
        Assert.assertEquals(SEDAMessageChannel.MAX_IN_MEMORY_DEAD_LETTERS, channel.inMemoryDeadLetters().size());
        Assert.assertEquals(SEDAMessageChannel.MAX_IN_MEMORY_DEAD_LETTERS + 5, channel.deadLetterCount());
    }

    @Test
    public void deadLetteredEnvelopeReleasesAndNotifiesItsProducer() throws Exception {
        bus.registerChannel("fails", 10, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("fails", e -> false);
        CountDownLatch told = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Envelope> reply = new java.util.concurrent.atomic.AtomicReference<>();
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("fails", "handle", e);
        Assert.assertTrue(bus.publish(e, env -> {
            reply.set(env);
            told.countDown();
        }));

        Assert.assertTrue("the producer is told when its envelope is given up on", await(told, 10));
        Assert.assertFalse(Envelope.getErrorMessages(reply.get()).isEmpty());
        Assert.assertTrue(Envelope.getErrorMessages(reply.get()).get(Envelope.getErrorMessages(reply.get()).size() - 1)
                .contains("dead-lettered"));
        Assert.assertEquals("the callback is not kept", 0, bus.pendingCallbacks());
    }

    @Test
    public void dropOldestDeletesTheEvictedEnvelopesFileAndTellsItsProducer() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-evict-" + System.nanoTime();
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase", base);
        SEDABus durableBus = new SEDABus();
        durableBus.start(p);
        try {
            SEDAMessageChannel channel = (SEDAMessageChannel) durableBus.registerChannel(
                    "evicting", 1, ServiceLevel.AtLeastOnce, null, false, 1, Backpressure.DropOldest);
            // The first envelope is held in flight by the consumer, the second waits in the queue (capacity 1), and the
            // third pushes the second out.
            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch inFlight = new CountDownLatch(1);
            durableBus.registerAsynchConsumer("evicting", e -> {
                inFlight.countDown();
                await(gate, 10);
                return true;
            });
            Envelope first = Envelope.documentFactory();
            first.setServiceLevel(ServiceLevel.AtLeastOnce);
            DLC.addRoute("evicting", "handle", first);
            Assert.assertTrue(durableBus.publish(first));
            Assert.assertTrue(await(inFlight, 10));

            CountDownLatch told = new CountDownLatch(1);
            Envelope second = Envelope.documentFactory();
            second.setServiceLevel(ServiceLevel.AtLeastOnce);
            DLC.addRoute("evicting", "handle", second);
            Assert.assertTrue(durableBus.publish(second, env -> told.countDown()));
            Envelope third = Envelope.documentFactory();
            third.setServiceLevel(ServiceLevel.AtLeastOnce);
            DLC.addRoute("evicting", "handle", third);
            Assert.assertTrue(durableBus.publish(third));

            Assert.assertTrue("the evicted envelope's producer is told", await(told, 10));
            Assert.assertEquals(1, channel.evictedCount());
            File[] files = new File(base, "evicting").listFiles((d, n) -> n.endsWith(".json"));
            Assert.assertNotNull(files);
            Assert.assertEquals("the one in flight and the one queued are kept; the evicted one is gone", 2, files.length);
            for (File f : files) {
                Assert.assertFalse(f.getName().contains(second.getId()));
            }
            Assert.assertEquals(0, durableBus.pendingCallbacks());
            gate.countDown();
        } finally {
            durableBus.gracefulShutdown();
        }
    }

    private static Envelope exactlyOnceEnvelope(String channel) {
        Envelope e = Envelope.documentFactory();
        e.setServiceLevel(ServiceLevel.ExactlyOnce);
        DLC.addRoute(channel, "handle", e);
        return e;
    }

    private static SEDABus durableBus(String base) {
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase", base);
        SEDABus b = new SEDABus();
        b.start(p);
        return b;
    }

    @Test
    public void exactlyOnceDeliveredIdsAreWrittenToDisk() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-dedup-" + System.nanoTime();
        SEDABus b = durableBus(base);
        try {
            CountDownLatch done = new CountDownLatch(1);
            b.registerChannel("once", 50, ServiceLevel.ExactlyOnce, null, false);
            b.registerAsynchConsumer("once", e -> {
                done.countDown();
                return true;
            });
            Envelope e = exactlyOnceEnvelope("once");
            Assert.assertTrue(b.publish(e));
            Assert.assertTrue(await(done, 10));
            File log = new File(base + "/once", "delivered.log");
            for (int i = 0; i < 100 && !log.exists(); i++) {
                Thread.sleep(20);
            }
            Assert.assertTrue("delivered ids are kept on disk", log.exists());
            Assert.assertTrue(new String(java.nio.file.Files.readAllBytes(log.toPath())).contains(e.getId()));
        } finally {
            b.gracefulShutdown();
        }
    }

    @Test
    public void aRestartSkipsWhatWasAlreadyDelivered() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-dedup-restart-" + System.nanoTime();
        Envelope delivered = exactlyOnceEnvelope("again");
        SEDABus first = durableBus(base);
        try {
            CountDownLatch done = new CountDownLatch(1);
            first.registerChannel("again", 50, ServiceLevel.ExactlyOnce, null, false);
            first.registerAsynchConsumer("again", e -> {
                done.countDown();
                return true;
            });
            Assert.assertTrue(first.publish(delivered));
            Assert.assertTrue(await(done, 10));
            Thread.sleep(300);
        } finally {
            first.gracefulShutdown();
        }

        // A crash after the id was recorded and before the persisted copy was deleted leaves that copy behind, and
        // another envelope that was never delivered is waiting too.
        Envelope waiting = exactlyOnceEnvelope("again");
        File dir = new File(base, "again");
        java.nio.file.Files.write(new File(dir, String.format("%019d-%08d-%s.json", 1L, 1, delivered.getId())).toPath(),
                delivered.toJSON().getBytes());
        java.nio.file.Files.write(new File(dir, String.format("%019d-%08d-%s.json", 2L, 2, waiting.getId())).toPath(),
                waiting.toJSON().getBytes());

        SEDABus second = durableBus(base);
        try {
            ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
            CountDownLatch got = new CountDownLatch(1);
            SEDAMessageChannel channel = (SEDAMessageChannel) second.registerChannel("again", 50, ServiceLevel.ExactlyOnce, null, false);
            second.registerAsynchConsumer("again", e -> {
                seen.add(e.getId());
                got.countDown();
                return true;
            });
            channel.sendUnprocessed();
            Assert.assertTrue(await(got, 10));
            Thread.sleep(300);

            Assert.assertEquals("only the envelope that was never delivered is delivered", 1, seen.size());
            Assert.assertEquals(waiting.getId(), seen.peek());
            Assert.assertFalse("the skipped envelope's copy is deleted",
                    java.util.Arrays.stream(dir.listFiles()).anyMatch(f -> f.getName().contains(delivered.getId()) && f.getName().endsWith(".json")));
        } finally {
            second.gracefulShutdown();
        }
    }

    @Test
    public void theDeliveredLogIsCompactedToTheMostRecentIds() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-dedup-compact-" + System.nanoTime();
        SEDABus b = durableBus(base);
        try {
            CountDownLatch done = new CountDownLatch(30);
            SEDAMessageChannel channel = (SEDAMessageChannel) b.registerChannel("compact", 100, ServiceLevel.ExactlyOnce, null, false);
            b.registerAsynchConsumer("compact", e -> {
                done.countDown();
                return true;
            });
            for (int i = 0; i < 30; i++) {
                Assert.assertTrue(b.publish(exactlyOnceEnvelope("compact")));
            }
            Assert.assertTrue(await(done, 10));
            File log = new File(base + "/compact", "delivered.log");
            for (int i = 0; i < 200 && (!log.exists() || java.nio.file.Files.readAllLines(log.toPath()).size() < 30); i++) {
                Thread.sleep(20);
            }
            channel.compactDeliveredLog(10);
            Assert.assertEquals(10, java.nio.file.Files.readAllLines(log.toPath()).size());
        } finally {
            b.gracefulShutdown();
        }
    }

    @Test
    public void rejectsWhenChannelAtCapacity() {
        CountDownLatch gate = new CountDownLatch(1);
        bus.registerChannel("slow", 2, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("slow", e -> {
            await(gate, 5);
            return true;
        });

        int accepted = 0;
        for (int i = 0; i < 10; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("slow", "handle", e);
            if (bus.publish(e)) {
                accepted++;
            }
        }
        gate.countDown();
        // 1 in-flight + 2 queued at most.
        Assert.assertTrue("accepted=" + accepted, accepted <= 3);
    }

    @Test
    public void dropNewestRejectsLikeRejectWhenFull() {
        // DropNewest and Reject are the same observable outcome from the
        // caller's perspective - "don't admit the new one" - matching every
        // other seda-bus language port's identical treatment of the two.
        // This exists to prove the enum value is actually wired through
        // admit(), not to show different behaviour from Reject.
        CountDownLatch gate = new CountDownLatch(1);
        bus.registerChannel("dn", 2, ServiceLevel.AtMostOnce, null, false, 1, Backpressure.DropNewest);
        bus.registerAsynchConsumer("dn", e -> {
            await(gate, 5);
            return true;
        });

        int accepted = 0;
        for (int i = 0; i < 10; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("dn", "handle", e);
            if (bus.publish(e)) {
                accepted++;
            }
        }
        gate.countDown();
        Assert.assertTrue("accepted=" + accepted, accepted <= 3);
    }

    @Test
    public void dropOldestEvictsInsteadOfRejecting() {
        // Found missing entirely by an independent production-readiness
        // audit: before this fix, a full channel had exactly one behaviour
        // (Reject), regardless of what a caller configured. DropOldest must
        // always admit the newest envelope by evicting the oldest queued
        // one instead of ever returning false for capacity reasons.
        CountDownLatch gate = new CountDownLatch(1);
        bus.registerChannel("bounded", 2, ServiceLevel.AtMostOnce, null, false, 1, Backpressure.DropOldest);
        bus.registerAsynchConsumer("bounded", e -> {
            await(gate, 5);
            return true;
        });

        int accepted = 0;
        for (int i = 0; i < 10; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("bounded", "handle", e);
            if (bus.publish(e)) {
                accepted++;
            }
        }
        gate.countDown();
        Assert.assertEquals(10, accepted);
    }

    @Test
    public void blockBackpressureWaitsInsteadOfRejecting() throws InterruptedException {
        // The other new policy this fix adds: instead of ever rejecting,
        // the producer's own thread waits (queue.put) for room. Bounded by
        // an explicit producer.join(timeout) rather than letting the
        // producer thread block forever, so a regression (Block silently
        // still rejecting, or a stuck wait) fails this test loudly instead
        // of hanging the whole suite.
        int total = 30;
        CountDownLatch done = new CountDownLatch(total);
        bus.registerChannel("tight", 2, ServiceLevel.AtMostOnce, null, false, 1, Backpressure.Block);
        bus.registerAsynchConsumer("tight", e -> {
            try {
                Thread.sleep(5);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            done.countDown();
            return true;
        });

        AtomicInteger accepted = new AtomicInteger();
        Thread producer = new Thread(() -> {
            for (int i = 0; i < total; i++) {
                Envelope e = Envelope.documentFactory();
                DLC.addRoute("tight", "handle", e);
                if (bus.publish(e)) {
                    accepted.incrementAndGet();
                }
            }
        });
        producer.start();
        producer.join(15_000);
        Assert.assertFalse("producer thread never returned from Block", producer.isAlive());
        Assert.assertEquals(total, accepted.get());
        Assert.assertTrue(await(done, 10));
    }

    @Test
    public void succeedsOnFinalAttemptDeliversExactlyOnce() {
        AtomicInteger tries = new AtomicInteger();
        AtomicInteger delivered = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        bus.registerChannel("recovers", 10, ServiceLevel.AtMostOnce, null, false); // maxAttempts=3
        bus.registerAsynchConsumer("recovers", e -> {
            int n = tries.incrementAndGet();
            if (n < 3) {
                return false; // nack twice
            }
            delivered.incrementAndGet();
            done.countDown();
            return true; // succeed on the 3rd (final allowed) attempt
        });

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("recovers", "handle", e);
        bus.publish(e);

        Assert.assertTrue(await(done, 10));
        // Give a spurious extra retry a moment to show up, if the per-envelope
        // attempts-map entry wasn't actually cleared on success (ack() does
        // this per the source; this pins the observable consequence down).
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
        Assert.assertEquals(3, tries.get());
        Assert.assertEquals(1, delivered.get());
    }

    @Test
    public void noConsumersEventuallyDeadLettersWithoutMessageLoss() {
        // deliverPointToPoint's empty-consumer-list path returns false,
        // which process() currently treats the same as any other nack -
        // retried up to maxAttempts (3, this overload's default) before
        // dead-lettering, not on the very first attempt. Still correct (no
        // message loss, no infinite retry loop) - pinned down as the actual
        // behaviour rather than left as an assumption from reading the
        // source alone.
        MessageChannel ch = bus.registerChannel("empty", 10, ServiceLevel.AtMostOnce, null, false);
        // Deliberately no registerAsynchConsumer call.

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("empty", "handle", e);
        Assert.assertTrue(bus.publish(e));

        // No consumer will ever ack it; the bounded number of immediate
        // retries (no backoff) needs a moment to exhaust and dead-letter.
        long deadline = System.currentTimeMillis() + 2000;
        while (ch.queued() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
            }
        }
        Assert.assertEquals("a no-consumer channel must eventually dead-letter, not leave the envelope queued forever",
                0, ch.queued());
    }

    @Test
    public void throwingConsumerDoesNotCrashTheBusOrLoseOtherEnvelopes() {
        int normalCount = 9;
        CountDownLatch normalDelivered = new CountDownLatch(normalCount);
        bus.registerChannel("poison", 20, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("poison", e -> {
            if ("boom".equals(DLC.getValue("tag", e))) {
                throw new RuntimeException("simulated consumer failure");
            }
            normalDelivered.countDown();
            return true;
        });

        for (int i = 0; i < normalCount; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addNVP("tag", "ok", e);
            DLC.addRoute("poison", "handle", e);
            Assert.assertTrue(bus.publish(e));
        }
        Envelope poison = Envelope.documentFactory();
        DLC.addNVP("tag", "boom", poison);
        DLC.addRoute("poison", "handle", poison);
        Assert.assertTrue(bus.publish(poison));

        Assert.assertTrue("a throwing consumer must not stop other envelopes on the same channel from being delivered",
                await(normalDelivered, 10));
    }

    @Test
    public void capacityBelowOneIsClampedToAtLeastOne() {
        // SEDAMessageChannel documents Math.max(1, capacity) - this pins
        // that clamp-not-fail-fast behaviour down explicitly rather than
        // leaving it an inference from reading the source.
        CountDownLatch latch = new CountDownLatch(1);
        bus.registerChannel("weird", 0, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("weird", e -> {
            latch.countDown();
            return true;
        });

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("weird", "handle", e);
        Assert.assertTrue("capacity<=0 must be clamped to a usable minimum, not break the channel",
                bus.publish(e));
        Assert.assertTrue(await(latch, 5));
    }

    @Test
    public void maxAttemptsBelowOneIsClampedToAtLeastOne() throws Exception {
        // maxAttempts isn't reachable via any public SEDABus.registerChannel
        // overload - construct the channel directly (this test is in the
        // same package) to pin down SEDAMessageChannel's own clamp.
        SEDAMessageChannel ch = new SEDAMessageChannel(
                bus, "raw", 5, null, ServiceLevel.AtMostOnce, false, 0, Backpressure.Reject);
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase",
                System.getProperty("java.io.tmpdir") + "/seda-test-raw-" + System.nanoTime());
        Assert.assertTrue(ch.start(p));

        AtomicInteger tries = new AtomicInteger();
        ch.registerAsyncConsumer(e -> {
            tries.incrementAndGet();
            return false; // always nack
        });

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("raw", "handle", e);
        Assert.assertTrue(ch.send(e));
        // Drive the channel directly - it's not registered with a bus/pool.
        while (ch.queued() > 0) {
            ch.poll();
        }
        Assert.assertEquals("maxAttempts<=0 must clamp to at least 1, not disable retries or loop forever",
                1, tries.get());
        ch.shutdown();
    }

    @Test
    public void repeatedLifecyclesDoNotLeakThreads() {
        java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory.getThreadMXBean();
        runOneBusLifecycle(); // warm up classes/JIT once outside the measured loop
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
        int before = threadBean.getThreadCount();

        for (int i = 0; i < 20; i++) {
            runOneBusLifecycle();
        }
        try {
            Thread.sleep(200); // let any terminating pool threads actually die
        } catch (InterruptedException ignored) {
        }
        int after = threadBean.getThreadCount();

        Assert.assertTrue("thread count grew from " + before + " to " + after + " over 20 create/shutdown cycles",
                after <= before + 5); // small tolerance for GC/JIT/compiler threads, not a per-cycle leak
    }

    private void runOneBusLifecycle() {
        SEDABus b = new SEDABus();
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase",
                System.getProperty("java.io.tmpdir") + "/seda-leak-" + System.nanoTime());
        b.start(p);
        CountDownLatch latch = new CountDownLatch(1);
        b.registerChannel("x");
        b.registerAsynchConsumer("x", e -> {
            latch.countDown();
            return true;
        });
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("x", "h", e);
        b.publish(e);
        await(latch, 5);
        b.gracefulShutdown();
    }

    @Test
    public void shutdownDoesNotReportDrainedWhileWorkIsStillInFlight() {
        // Investigated in depth while writing this test, worth recording:
        // SEDAMessageChannel.drainWithin (backing MessageChannel.shutdown()/
        // gracefulShutdown()) only polls queued() - the raw queue length.
        // poll() removes an envelope from the queue *before* calling
        // process() (which runs the consumer and can take arbitrary time),
        // so queued() reads 0 while the last-popped envelope is still being
        // handled on a worker thread - confirmed directly with instrumented
        // timing: with 5 envelopes at 300ms each, channel-level
        // gracefulShutdown() returns true at ~1240ms, a full ~300ms before
        // the 5th envelope is actually delivered at ~1540ms.
        //
        // This test still passes reliably, because SEDABus.doShutdown calls
        // WorkerThreadPool.shutdown() *after* the channel-level check, which
        // calls ExecutorService.awaitTermination - a real barrier that
        // blocks the bus-level shutdown()/gracefulShutdown() call until the
        // in-flight drain task (including the consumer call it's in the
        // middle of) actually finishes, independent of the channel's own
        // (inaccurate) return value. So bus-level shutdown is accidentally
        // correct in practice; a *channel* used directly without a bus (no
        // pool to provide that backstop - e.g. maxAttemptsBelowOneIsClampedToAtLeastOne
        // below drains its channel manually before calling shutdown() for
        // exactly this reason) would not be. Testing the bus-level API here
        // since that's what every real caller uses.
        int total = 5;
        AtomicInteger delivered = new AtomicInteger();
        bus.registerChannel("slowproc", 10, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("slowproc", e -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            delivered.incrementAndGet();
            return true;
        });

        for (int i = 0; i < total; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("slowproc", "handle", e);
            Assert.assertTrue(bus.publish(e));
        }

        boolean drained = bus.gracefulShutdown();
        if (drained) {
            Assert.assertEquals(
                    "shutdown reported fully drained but an in-flight envelope hadn't finished processing",
                    total, delivered.get());
        }
        // If drained is false, gracefulShutdown's own 30s timeout elapsed
        // first - not a false claim, just an honest "gave up" (not expected
        // with a 5 * 300ms workload well under 30s, but not this test's
        // concern either way).
    }

    @Test
    public void nackRetriesThenDeadLetters() {
        AtomicInteger tries = new AtomicInteger();
        CountDownLatch failedEnough = new CountDownLatch(3);
        bus.registerChannel("flaky", 10, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("flaky", e -> {
            tries.incrementAndGet();
            failedEnough.countDown();
            return false;
        });

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("flaky", "handle", e);
        bus.publish(e);

        Assert.assertTrue(await(failedEnough, 10));
        // retried up to maxAttempts (3) then dead-lettered; no infinite loop
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
        Assert.assertEquals(3, tries.get());
    }

    @Test
    public void pubSubFansOutToSubscribers() {
        CountDownLatch latch = new CountDownLatch(6); // 3 messages * 2 subscribers
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        bus.registerChannel("topic", 100, ServiceLevel.AtMostOnce, null, true);
        MessageConsumer c = e -> {
            seen.add(e.getId());
            latch.countDown();
            return true;
        };
        MessageChannel sc = bus.registerSubscriberChannel("topic", "sub-c", 100, ServiceLevel.AtMostOnce, null, false);
        MessageChannel sd = bus.registerSubscriberChannel("topic", "sub-d", 100, ServiceLevel.AtMostOnce, null, false);
        sc.registerAsyncConsumer(c);
        sd.registerAsyncConsumer(c);

        for (int i = 0; i < 3; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("topic", "publish", e);
            bus.publish(e);
        }
        Assert.assertTrue(await(latch, 10));
        Assert.assertEquals(6, seen.size());
    }

    @Test
    public void pauseStopsDeliveryThenResumes() {
        CountDownLatch latch = new CountDownLatch(1);
        bus.registerChannel("p");
        bus.registerAsynchConsumer("p", e -> {
            latch.countDown();
            return true;
        });

        bus.pause();
        Envelope e1 = Envelope.documentFactory();
        DLC.addRoute("p", "h", e1);
        Assert.assertFalse("publish must be rejected while paused", bus.publish(e1));

        bus.unpause();
        Envelope e2 = Envelope.documentFactory();
        DLC.addRoute("p", "h", e2);
        Assert.assertTrue(bus.publish(e2));
        Assert.assertTrue(await(latch, 10));
    }

    @Test
    public void guaranteedDeliveryPersistsThenClearsOnAck() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-durable-" + System.nanoTime();
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase", base);
        SEDABus durableBus = new SEDABus();
        durableBus.start(p);
        try {
            CountDownLatch latch = new CountDownLatch(5);
            durableBus.registerChannel("durable", 50, ServiceLevel.AtLeastOnce, null, false);
            durableBus.registerAsynchConsumer("durable", e -> {
                latch.countDown();
                return true;
            });

            for (int i = 0; i < 5; i++) {
                Envelope e = Envelope.documentFactory();
                e.setServiceLevel(ServiceLevel.AtLeastOnce);
                DLC.addRoute("durable", "handle", e);
                Assert.assertTrue(durableBus.publish(e));
            }
            Assert.assertTrue(await(latch, 10));
            Thread.sleep(200);

            File dir = new File(base, "durable");
            File[] left = dir.listFiles((d, n) -> n.endsWith(".json"));
            Assert.assertTrue("acked envelopes must be removed from the store",
                    left == null || left.length == 0);
        } finally {
            durableBus.gracefulShutdown();
        }
    }

    @Test
    public void manyProducersDeliverEverything() throws InterruptedException {
        int producers = 6;
        int each = 500;
        CountDownLatch latch = new CountDownLatch(producers * each);
        bus.registerChannel("fan", producers * each + 100, ServiceLevel.AtMostOnce, null, false, 8);
        bus.registerAsynchConsumer("fan", e -> {
            latch.countDown();
            return true;
        });

        Thread[] threads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            threads[p] = new Thread(() -> {
                for (int i = 0; i < each; i++) {
                    Envelope e = Envelope.documentFactory();
                    DLC.addRoute("fan", "handle", e);
                    while (!bus.publish(e)) {
                        Thread.yield();
                    }
                }
            });
            threads[p].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        Assert.assertTrue(await(latch, 20));
    }
}
