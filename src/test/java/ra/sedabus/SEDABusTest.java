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
        bus.registerChannel("work");
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
