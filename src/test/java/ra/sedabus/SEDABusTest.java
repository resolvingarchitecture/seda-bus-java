package ra.sedabus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.MessageConsumer;

import java.util.Properties;
import java.util.logging.Logger;

public class SEDABusTest {

    private Logger LOG = Logger.getLogger(SEDABusTest.class.getName());

    protected SEDABus bus;

    @Before
    public void init() {
        LOG.info("Init...");
        Properties props = new Properties();
        bus = SEDABus.getInstance(props);
        bus.start(props);
    }

    @After
    public void tearDown() {
        LOG.info("Teardown...");
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
        bus.registerConsumer("B", consumer);
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

    @Test(timeout = 1000)
    public void exactlyOnce() {
        LOG.info("Exactly Once...");

        Assert.assertTrue(true);
    }



}
