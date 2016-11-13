package nuke.derbysoft.com.test;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import scala.Option;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static akka.actor.SupervisorStrategy.*;

/**
 * http://img.blog.csdn.net/20150312213012535?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvcm93YW5oYW9h/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/Center
 * <p>
 * Created by passyt on 16-11-13.
 */
public class PiApp {

    private static AtomicBoolean THROW_ERROR = new AtomicBoolean(true);
    private static final SupervisorStrategy strategy = new OneForOneStrategy(
            1,
            Duration.create("1 minute"),
            t -> {
                if (t instanceof IllegalStateException) {
                    return restart();
                } else if (t instanceof NullPointerException) {
                    return restart();
                } else if (t instanceof IllegalArgumentException) {
                    return stop();
                } else {
                    return escalate();
                }
            }
    );

    public static void main(String... args) {
        ActorSystem system = ActorSystem.create("PI-System");
        ActorRef listener = system.actorOf(Props.create(PiListener.class), "listener");
        ActorRef master = system.actorOf(Props.create(Master.class, 4, 10000, 100000, listener), "master");
        master.tell(new StartMessage(), ActorRef.noSender());
    }

    private static class StartMessage implements Serializable {
    }

    private static class WorkMessage implements Serializable {

        private final int start;
        private final int numberOfElements;

        public WorkMessage(int start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        public int getStart() {
            return start;
        }

        public int getNumberOfElements() {
            return numberOfElements;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("WorkMessage{");
            sb.append("start=").append(start);
            sb.append(", numberOfElements=").append(numberOfElements);
            sb.append('}');
            return sb.toString();
        }
    }

    private static class ResultMessage implements Serializable {

        private final Double pi;
        private final Duration duration;

        public ResultMessage(Double pi, Duration duration) {
            this.pi = pi;
            this.duration = duration;
        }

        public Double getPi() {
            return pi;
        }

        public Duration getDuration() {
            return duration;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ResultMessage{");
            sb.append("pi=").append(pi);
            sb.append(", duration=").append(duration);
            sb.append('}');
            return sb.toString();
        }
    }

    private static class Worker extends AbstractActor {

        private final LoggingAdapter log = Logging.getLogger(context().system(), this);

        public Worker() {
            receive(
                    ReceiveBuilder
                            .match(
                                    WorkMessage.class, workMessage -> {
                                        log.info("Received message: {}", workMessage);
                                        Double result = calculatePi(workMessage.getStart(), workMessage.getNumberOfElements());
                                        sender().tell(result, self());
                                    })
                            .matchAny(other ->
                                    log.warning("received unknown message: {}", other)
                            ).build()
            );
        }

        private Double calculatePi(int start, int numberOfElements) {
            if (THROW_ERROR.getAndSet(false)) {
                throw new IllegalStateException("an expected exception");
            }

            double result = 0;
            for (int i = start * numberOfElements; i <= (start + 1) + numberOfElements; i++) {
                result += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
            }
            return result;
        }

        @Override
        public void preRestart(Throwable reason, Option<Object> message) throws Exception {
            log.error(reason, "Pre restarting...{}", this);
            self().tell(message.get(), sender());
            super.preRestart(reason, message);
        }

    }

    private static class Master extends AbstractActor {

        private final LoggingAdapter log = Logging.getLogger(context().system(), this);
        private final int numberOfMessages;
        private final int numberOfElements;

        private long start;
        private double pi;
        private int numberOfResults;

        public Master(int numberOfWorkers, int numberOfMessages, int numberOfElements, ActorRef listener) {
            this.numberOfMessages = numberOfMessages;
            this.numberOfElements = numberOfElements;

            ActorRef workerRouter = this.getContext().actorOf(Props.create(Worker.class)/*.withRouter(new FromConfig())*/, "workerRouter");

            receive(
                    ReceiveBuilder
                            .match(
                                    StartMessage.class, startMessage -> {
                                        start = System.currentTimeMillis();
                                        for (int start = 0; start < numberOfMessages; start++) {
                                            workerRouter.tell(new WorkMessage(start, numberOfElements), self());
                                        }
                                    }
                            )
                            .match(Double.class, result -> {
                                pi += result;
                                numberOfResults++;
                                if (numberOfResults == numberOfMessages) {
                                    Duration duration = Duration.create(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                                    listener.tell(new ResultMessage(pi, duration), self());
                                }
                            })
                            .matchAny(other ->
                                    log.warning("received unknown message: {}", other)
                            ).build()
            );
        }

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public void preRestart(Throwable reason, Option<Object> message) throws Exception {
            log.error(reason, "Restarting...{}", this);
            super.preRestart(reason, message);
        }

    }

    private static class PiListener extends AbstractActor {

        private final LoggingAdapter log = Logging.getLogger(context().system(), this);

        public PiListener() {
            receive(
                    ReceiveBuilder
                            .match(ResultMessage.class, resultMessage -> {
                                System.out.println("Pi = " + resultMessage.getPi());
                                System.out.println("Cost " + resultMessage.getDuration());
                                context().system().shutdown();
                            })
                            .matchAny(other ->
                                    log.warning("received unknown message: {}", other)
                            ).build()
            );
        }

    }

}
