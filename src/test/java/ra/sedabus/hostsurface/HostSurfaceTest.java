package ra.sedabus.hostsurface;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.service.ServiceLevel;
import ra.sedabus.Backpressure;
import ra.sedabus.SEDABus;
import ra.sedabus.SEDAMessageChannel;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * What a host can do with the bus from outside its package: nothing here is package-private, so this test fails to
 * compile if the settings, counters or listener ever become unreachable again.
 */
public class HostSurfaceTest {

    private SEDABus bus;

    private SEDABus start(Properties extra) {
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase", System.getProperty("java.io.tmpdir") + "/seda-host-" + System.nanoTime());
        p.putAll(extra);
        SEDABus b = new SEDABus();
        Assert.assertTrue(b.start(p));
        return b;
    }

    @Before
    public void setUp() {
        Thread.interrupted();
        bus = start(new Properties());
    }

    @After
    public void tearDown() {
        bus.gracefulShutdown();
    }

    private static boolean await(CountDownLatch l) {
        try {
            return l.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Test
    public void aHostCanReachAChannelItsSettingsAndItsCounters() throws Exception {
        bus.registerChannel("host", 10, ServiceLevel.AtMostOnce, null, false);
        bus.registerAsynchConsumer("host", e -> false);
        SEDAMessageChannel channel = bus.getChannel("host");
        Assert.assertNotNull(channel);
        Assert.assertNull(bus.getChannel("nope"));
        channel.setRetryBackoff(5, 10);

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("host", "handle", e);
        bus.publish(e);
        for (int i = 0; i < 200 && channel.deadLetterCount() == 0; i++) {
            Thread.sleep(20);
        }

        Assert.assertEquals(1, channel.deadLetterCount());
        Assert.assertEquals(1, channel.inMemoryDeadLetters().size());
        Assert.assertEquals(0, channel.pendingRetries());
        Assert.assertEquals(0, channel.evictedCount());
    }

    @Test
    public void aListenerIsToldWhenAnEnvelopeIsGivenUpOnAndItsFailureDoesNotBreakTheBus() throws Exception {
        ConcurrentLinkedQueue<String> told = new ConcurrentLinkedQueue<>();
        CountDownLatch twice = new CountDownLatch(2);
        bus.setDeadLetterListener((channel, envelope, reason) -> {
            told.add(channel + ":" + envelope.getId() + ":" + reason);
            twice.countDown();
            throw new IllegalStateException("a listener that misbehaves");
        });
        bus.registerChannel("listened", 10, ServiceLevel.AtMostOnce, null, false);
        bus.getChannel("listened").setRetryBackoff(5, 10);
        bus.registerAsynchConsumer("listened", e -> false);

        Envelope first = Envelope.documentFactory();
        DLC.addRoute("listened", "handle", first);
        Envelope second = Envelope.documentFactory();
        DLC.addRoute("listened", "handle", second);
        bus.publish(first);
        bus.publish(second);

        Assert.assertTrue("every give-up reaches the listener, whatever it throws", await(twice));
        Assert.assertTrue(told.stream().anyMatch(t -> t.startsWith("listened:" + first.getId())));
        Assert.assertTrue(told.stream().allMatch(t -> t.contains("dead-lettered")));
    }

    @Test
    public void anEvictionReachesTheListenerToo() throws Exception {
        ConcurrentLinkedQueue<String> told = new ConcurrentLinkedQueue<>();
        bus.setDeadLetterListener((channel, envelope, reason) -> told.add(reason));
        bus.registerChannel("evicts", 1, ServiceLevel.AtMostOnce, null, false, 1, Backpressure.DropOldest);
        CountDownLatch inFlight = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        bus.registerAsynchConsumer("evicts", e -> {
            inFlight.countDown();
            try {
                gate.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            return true;
        });
        for (int i = 0; i < 3; i++) {
            Envelope e = Envelope.documentFactory();
            DLC.addRoute("evicts", "handle", e);
            Assert.assertTrue(bus.publish(e));
            if (i == 0) {
                Assert.assertTrue(await(inFlight));
            }
        }
        gate.countDown();

        Assert.assertEquals(1, bus.getChannel("evicts").evictedCount());
        Assert.assertEquals(1, told.size());
        Assert.assertTrue(told.peek().contains("DropOldest"));
    }

    @Test
    public void retrySettingsComeFromTheBusProperties() {
        Properties p = new Properties();
        p.setProperty("ra.sedabus.retry.baseMs", "400");
        p.setProperty("ra.sedabus.retry.maxMs", "400");
        p.setProperty("ra.sedabus.retry.handOverAttempts", "4");
        SEDABus configured = start(p);
        try {
            configured.registerChannel("configured", 10, ServiceLevel.AtMostOnce, null, false);
            SEDAMessageChannel channel = configured.getChannel("configured");
            Assert.assertEquals(4, channel.handOverAttempts());
            long delay = channel.backoffMs(1);
            Assert.assertTrue("delay " + delay, delay >= 320 && delay <= 480);
            Assert.assertEquals("the default is unchanged on another bus", 10, bus.registerChannel("other") == null ? -1
                    : bus.getChannel("other").handOverAttempts());
        } finally {
            configured.gracefulShutdown();
        }
    }

    private static long retryThreads() {
        return Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith("seda-retry-") && t.isAlive()).count();
    }

    @Test
    public void eachBusOwnsItsRetryTimerAndStopsItAtShutdown() throws Exception {
        long before = retryThreads();
        SEDABus other = start(new Properties());
        // The timer thread starts with its first task: give the bus work that has to wait.
        other.registerChannel("waits", 10, ServiceLevel.AtMostOnce, null, false);
        other.getChannel("waits").setRetryBackoff(50, 50);
        other.registerAsynchConsumer("waits", e -> false);
        Envelope e = Envelope.documentFactory();
        DLC.addRoute("waits", "handle", e);
        other.publish(e);
        for (int i = 0; i < 100 && retryThreads() <= before; i++) {
            Thread.sleep(10);
        }
        Assert.assertTrue("the second bus has a timer thread of its own", retryThreads() > before);

        other.gracefulShutdown();
        for (int i = 0; i < 200 && retryThreads() > before; i++) {
            Thread.sleep(10);
        }
        Assert.assertEquals("its timer stops with it, and the first bus's is untouched", before, retryThreads());
    }

    private static Envelope failingEnvelope(String channel, ServiceLevel level) {
        Envelope e = Envelope.documentFactory();
        e.setServiceLevel(level);
        DLC.addRoute(channel, "handle", e);
        return e;
    }

    @Test
    public void aNonPersistedEnvelopeWaitingToBeRetriedWhenTheBusStopsIsGivenUpOnNotLostSilently() throws Exception {
        SEDABus b = start(new Properties());
        ConcurrentLinkedQueue<String> reasons = new ConcurrentLinkedQueue<>();
        b.setDeadLetterListener((channel, envelope, reason) -> reasons.add(reason));
        b.registerChannel("wait-memory", 10, ServiceLevel.AtMostOnce, null, false);
        SEDAMessageChannel channel = b.getChannel("wait-memory");
        channel.setRetryBackoff(30_000, 30_000); // far longer than the shutdown waits
        b.registerAsynchConsumer("wait-memory", e -> false);
        Assert.assertTrue(b.publish(failingEnvelope("wait-memory", ServiceLevel.AtMostOnce)));
        for (int i = 0; i < 200 && channel.pendingRetries() == 0; i++) {
            Thread.sleep(10);
        }
        Assert.assertEquals(1, channel.pendingRetries());

        Assert.assertFalse("not drained: a retry was still waiting", b.shutdown());

        Assert.assertEquals(0, channel.pendingRetries());
        Assert.assertEquals(1, reasons.size());
        Assert.assertTrue(reasons.peek().contains("stopped before this envelope could be retried"));
        Assert.assertEquals(1, channel.inMemoryDeadLetters().size());
    }

    @Test
    public void aPersistedEnvelopeWaitingToBeRetriedWhenTheBusStopsIsKeptForTheNextStart() throws Exception {
        String base = System.getProperty("java.io.tmpdir") + "/seda-host-keep-" + System.nanoTime();
        Properties p = new Properties();
        p.setProperty("ra.sedabus.channel.locationBase", base);
        SEDABus b = new SEDABus();
        Assert.assertTrue(b.start(p));
        b.registerChannel("wait-disk", 10, ServiceLevel.AtLeastOnce, null, false);
        SEDAMessageChannel channel = b.getChannel("wait-disk");
        channel.setRetryBackoff(30_000, 30_000);
        b.registerAsynchConsumer("wait-disk", e -> false);
        Assert.assertTrue(b.publish(failingEnvelope("wait-disk", ServiceLevel.AtLeastOnce)));
        for (int i = 0; i < 200 && channel.pendingRetries() == 0; i++) {
            Thread.sleep(10);
        }
        Assert.assertEquals(1, channel.pendingRetries());

        b.shutdown();

        java.io.File[] kept = new java.io.File(base, "wait-disk").listFiles((d, n) -> n.endsWith(".json") && !n.equals("deadLetter.json"));
        Assert.assertNotNull(kept);
        Assert.assertEquals("its copy stays on disk, to be replayed", 1, kept.length);
        Assert.assertEquals(0, channel.deadLetterCount());
    }

    @Test
    public void aStoppedBusAcceptsNothingNew() {
        bus.registerChannel("late", 10, ServiceLevel.AtMostOnce, null, false);
        bus.shutdown();

        Envelope e = Envelope.documentFactory();
        DLC.addRoute("late", "handle", e);
        Assert.assertFalse(bus.publish(e));
    }
}
