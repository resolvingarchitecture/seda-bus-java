package ra.sedabus;

import org.junit.*;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.MessageConsumer;
import ra.common.ServiceLevel;
import ra.util.Wait;

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
    }

    @AfterClass
    public static void tearDown() {
        LOG.info("Teardown...");
        bus.gracefulShutdown();
    }

//    @Test
//    public void verifyPointToPoint() {
//        final int id = 1234;
//        MessageConsumer consumer = new MessageConsumer() {
//            @Override
//            public boolean receive(Envelope envelope) {
//                LOG.info("Received envelope (id="+envelope.getId()+")");
//                Assert.assertTrue(id == envelope.getId());
//                return true;
//            }
//        };
//        bus.registerChannel("A");
//        bus.registerAsynchConsumer("A", consumer);
//        Envelope env = Envelope.documentFactory(id);
//        DLC.addRoute("A","Send", env);
//        bus.publish(env);
//        Wait.aSec(2);
//    }

    @Test
    public void verifyPubSub() {
        final int id = 5678;
        MessageConsumer consumerC = new MessageConsumer() {
            @Override
            public boolean receive(Envelope envelope) {
                LOG.info("Consumer C received envelope (id="+envelope.getId()+")");
                Assert.assertTrue(id == envelope.getId());
                return true;
            }
        };
        MessageConsumer consumerD = new MessageConsumer() {
            @Override
            public boolean receive(Envelope envelope) {
                LOG.info("Consumer D received envelope (id="+envelope.getId()+")");
                Assert.assertTrue(id == envelope.getId());
                return true;
            }
        };
        bus.registerChannel("B", 100, ServiceLevel.AtLeastOnce, null, true);
        MessageChannel mcC = bus.registerSubscriberChannel("B", "C", 10, ServiceLevel.AtLeastOnce, null, false);
        mcC.registerAsyncConsumer(consumerC);
        MessageChannel mcD = bus.registerSubscriberChannel("B", "D", 10, ServiceLevel.AtLeastOnce, null, false);
        mcD.registerAsyncConsumer(consumerD);
        Envelope env = Envelope.documentFactory(id);
        DLC.addRoute("B","Send", env);
        bus.publish(env);
        Wait.aSec(2);
    }

//    @Test
//    public void atMostOnce() {
//        LOG.info("At Most Once...");
//
//        Assert.assertTrue(true);
//    }
//
//    @Test
//    public void atLeastOnce() {
//        LOG.info("At Least Once...");
//
//        Assert.assertTrue(true);
//    }
//
//    @Test
//    public void exactlyOnce() {
//        LOG.info("Exactly Once...");
//
//        Assert.assertTrue(true);
//    }

}
