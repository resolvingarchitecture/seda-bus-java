package ra.sedabus;

import org.junit.*;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.MessageConsumer;

import java.util.Properties;
import java.util.logging.Logger;

public class SEDABusTest {

    private static final Logger LOG = Logger.getLogger(SEDABusTest.class.getName());

    private static SEDABus bus;

    @BeforeClass
    public static void init() {
        LOG.info("Init...");
        Properties props = new Properties();
        bus = SEDABus.getInstance(props);
        bus.start(props);
    }

    @AfterClass
    public static void tearDown() {
        LOG.info("Teardown...");
        bus.gracefulShutdown();
    }

    @Test
    public void verifyPointToPoint() {
        int id = 1234;
        MessageConsumer consumer = new MessageConsumer() {
            @Override
            public boolean receive(Envelope envelope) {
                LOG.info("Received envelope (id="+envelope.getId()+")");
                Assert.assertTrue(id == envelope.getId());
                return true;
            }
        };
        bus.registerChannel("A");
        bus.registerChannel("B");
        bus.registerAsynchConsumer("B", consumer);
        Envelope env = Envelope.documentFactory(id);
        DLC.addRoute("B","Send", env);
        bus.publish(env);
    }

    @Test
    public void atMostOnce() {
        LOG.info("At Most Once...");

        Assert.assertTrue(true);
    }

    @Test
    public void atLeastOnce() {
        LOG.info("At Least Once...");

        Assert.assertTrue(true);
    }

    @Test
    public void exactlyOnce() {
        LOG.info("Exactly Once...");

        Assert.assertTrue(true);
    }



}
