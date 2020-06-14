package ra.sedabus;

import ra.common.BaseService;
import ra.common.Envelope;
import ra.common.MessageConsumer;
import ra.common.route.Route;
import ra.util.AppThread;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Worker Thread for moving messages from clients to the message channel and then to services and back.
 *
 * @author objectorange
 */
final class WorkerThread extends AppThread {

    private static final Logger LOG = Logger.getLogger(WorkerThread.class.getName());

    private MessageChannel channel;
    private MessageConsumer consumer;

    public WorkerThread(MessageChannel channel, MessageConsumer consumer) {
        super();
        this.channel = channel;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        LOG.finer(Thread.currentThread().getName() + ": Channel waiting to receive next message...");
        Envelope e = channel.receive();
        LOG.finer(Thread.currentThread().getName() + ": Channel received message; processing...");
        if (e.replyToClient()) {
            // Service Reply to client
            LOG.finer(Thread.currentThread().getName() + ": Requesting client notify...");
//            clientAppManager.notify(e);
        } else {
            MessageConsumer consumer = null;
            Route route = e.getRoute();
//            if(route == null || route.getRouted()) {
//                consumer = services.get(OrchestrationService.class.getName());
//            } else {
//                consumer = services.get(route.getService());
//                if (consumer == null) {
//                    // Service name provided is not registered.
//                    LOG.warning(Thread.currentThread().getName() + ": Route found in header; Service not registered; Please register service: "+route.getService()+"\n\tCurrent Registered Services: "+services);
//                    return;
//                }
//            }
            boolean received = false;
            int maxSendAttempts = 3;
            int sendAttempts = 0;
            int waitBetweenMillis = 1000;
            while (!received && sendAttempts < maxSendAttempts) {
                if (consumer.receive(e)) {
                    LOG.finer(Thread.currentThread().getName() + ": Envelope received by service, acknowledging with channel...");
                    channel.ack(e);
                    LOG.finer(Thread.currentThread().getName() + ": Channel Acknowledged.");
                    received = true;
                } else {
                    synchronized (this) {
                        try {
                            this.wait(waitBetweenMillis);
                        } catch (InterruptedException ex) {

                        }
                    }
                }
                sendAttempts++;
            }
            if(!received) {
                // TODO: Need to move the failed Envelope to a log where it can be retried later
                LOG.warning("Failed 3 attempts to send Envelope (id="+e.getId()+") to Service: ");
            }
        }
    }
}
