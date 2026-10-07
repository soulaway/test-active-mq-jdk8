package com.test.activemq.jdk8;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Forwards messages from queueIn to queueOut on Amazon MQ (ActiveMQ, OpenWire over SSL).
 *
 * How it works:
 *   - ONE transacted session is used for both the consumer and the producer.
 *   - The forwarder blocks until a message shows up in queueIn, then drains everything that is already
 *     available (up to --batch-size, served from the consumer's prefetch buffer, no extra round trips),
 *     sends every message to queueOut and finally calls session.commit() ONCE per batch.
 *   - commit() is the acknowledgement: the consumed messages are acked and the forwarded copies are
 *     published atomically. If anything fails the batch is rolled back, so nothing is lost
 *     (the messages stay in queueIn and are redelivered) and nothing is half-forwarded.
 *   - Sends inside a transaction are asynchronous, so the whole batch is pipelined to the broker and the
 *     only synchronous wait is the commit.
 *   - Messages are forwarded as received (the body is not unmarshalled); delivery mode, priority and the
 *     remaining time-to-live are preserved. Messages that already expired are consumed and dropped.
 *
 * Usage:
 *   java -jar mq-test.jar [-v] --url <brokerUrl> --user <user> --password <pass>
 *                         [--queue-in <name>] [--queue-out <name>] [--batch-size <n>] [--prefetch <n>]
 *                         [--timeout <ms>] [--once]
 *
 * Every option can also come from an env var: MQ_URL, MQ_USER, MQ_PASSWORD, MQ_QUEUE_IN, MQ_QUEUE_OUT,
 * MQ_BATCH_SIZE, MQ_PREFETCH, MQ_TIMEOUT_MS.
 *
 * Runs until Ctrl+C / SIGTERM (the batch in flight is finished and committed first), or with --once until
 * queueIn has been idle for --timeout ms.
 *
 * Exit code: 0 = OK (with --once: at least one message forwarded), 1 = failed, 2 = bad arguments.
 */
public class Forwarder {

    /** Consecutive failed batches before giving up (a failover: URL normally recovers within this). */
    private static final int MAX_CONSECUTIVE_FAILURES = 5;
    private static final long STATS_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private static volatile boolean running = true;

    private static final class Settings {
        String url;
        String user;
        String password;
        String queueIn;
        String queueOut;
        int batchSize;
        int prefetch;
        long timeoutMs;
        boolean once;
        boolean verbose;
    }

    public static void main(String[] args) {
        if (has(args, "-h") || has(args, "--help")) {
            usage();
            return;
        }

        Settings s = new Settings();
        s.verbose = has(args, "-v") || has(args, "--verbose");
        s.once = has(args, "--once");
        s.url = opt(args, "--url", "MQ_URL", null);
        s.user = opt(args, "--user", "MQ_USER", null);
        s.password = opt(args, "--password", "MQ_PASSWORD", null);
        s.queueIn = opt(args, "--queue-in", "MQ_QUEUE_IN", "mq-test-queue-in");
        s.queueOut = opt(args, "--queue-out", "MQ_QUEUE_OUT", "mq-test-queue-out");

        try {
            s.batchSize = Integer.parseInt(opt(args, "--batch-size", "MQ_BATCH_SIZE", "500"));
            // prefetch >= batch size, so a whole batch can be taken from memory without waiting for the broker
            s.prefetch = Integer.parseInt(opt(args, "--prefetch", "MQ_PREFETCH", String.valueOf(s.batchSize)));
            s.timeoutMs = Long.parseLong(opt(args, "--timeout", "MQ_TIMEOUT_MS", "10000"));
        } catch (NumberFormatException e) {
            System.err.println("Invalid number: " + e.getMessage() + "\n");
            usage();
            System.exit(2);
        }

        if (s.url == null || s.user == null || s.password == null) {
            System.err.println("Missing --url / --user / --password (or MQ_URL / MQ_USER / MQ_PASSWORD)\n");
            usage();
            System.exit(2);
        }

        if (s.batchSize < 1 || s.prefetch < 1 || s.timeoutMs < 1) {
            System.err.println("--batch-size, --prefetch and --timeout must be positive\n");
            usage();
            System.exit(2);
        }

        if (s.queueIn.equals(s.queueOut)) {
            System.err.println("--queue-in and --queue-out must differ (otherwise messages would loop forever)\n");
            System.exit(2);
        }

        // Must happen BEFORE the first logger is created and before any SSL class is touched.
        configureLogging(s.verbose);
        Logger log = LoggerFactory.getLogger(Forwarder.class);

        // Graceful stop: let the batch in flight finish and commit before the JVM exits.
        final CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                done.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown-hook"));

        boolean ok;
        try {
            ok = runForwarder(log, s);
        } finally {
            done.countDown();
        }
        log.info("RESULT: {}", ok ? "PASS" : "FAIL");
        System.exit(ok ? 0 : 1);

    }

    private static boolean runForwarder(Logger log, Settings s) {
        String brokerUrl = s.url;
        if (s.verbose) {
            log.debug("java.version={} java.vendor={}", System.getProperty("java.version"), System.getProperty("java.vendor"));
            logDnsResolution(log, s.url);
            brokerUrl = withTransportTrace(log, s.url);
        }
        log.info("Connecting to {} as '{}' ({} -> {}, batch-size {}, prefetch {}{})",
                brokerUrl, s.user, s.queueIn, s.queueOut, s.batchSize, s.prefetch, s.once ? ", once" : "");

        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory(brokerUrl);
        factory.setUserName(s.user);
        factory.setPassword(s.password);
        factory.getPrefetchPolicy().setQueuePrefetch(s.prefetch);

        long forwarded = 0;
        long expired = 0;
        long batches = 0;
        int failures = 0;
        boolean ok = true;
        long start = System.nanoTime();
        long lastActivity = start;
        long lastStats = start;

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.setExceptionListener(e -> log.error("Async connection exception", e));
            connection.start();

            ConnectionMetaData meta = connection.getMetaData();
            log.info("Connected. Provider: {} {}", meta.getJMSProviderName(), meta.getProviderVersion());

            // One transacted session for consumer AND producer: commit() = ack of queueIn + publish to queueOut.
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queueIn = session.createQueue(s.queueIn);
            Queue queueOut = session.createQueue(s.queueOut);
            MessageConsumer consumer = session.createConsumer(queueIn);
            MessageProducer producer = session.createProducer(queueOut);

            // Poll in short slices so Ctrl+C is noticed quickly; --timeout only matters for --once.
            long pollMs = Math.min(s.timeoutMs, 1000L);
            long idleLimitNanos = TimeUnit.MILLISECONDS.toNanos(s.timeoutMs);

            while (running) {
                long now = System.nanoTime();
                if (s.once && now - lastActivity >= idleLimitNanos) {
                    break;
                }
                if (now - lastStats >= STATS_INTERVAL_NANOS) {
                    lastStats = now;
                    log.info("Forwarded so far: {} messages in {} batches ({} msg/s)",
                            forwarded, batches, rate(forwarded, now - start));
                }

                // Wait for the first message ...
                Message m = consumer.receive(pollMs);
                if (m == null) {
                    continue;
                }

                // ... then take everything that is already there, up to batchSize, and forward it as one unit.
                int received = 0;
                int sent = 0;
                try {
                    while (m != null) {
                        received++;
                        if (forward(producer, m)) {
                            sent++;
                        }
                        if (received >= s.batchSize) {
                            break;
                        }
                        m = consumer.receiveNoWait();
                    }
                    session.commit(); // acknowledges the consumed messages and publishes the forwarded ones
                } catch (Exception e) {
                    failures++;
                    lastActivity = System.nanoTime();
                    log.error("Batch of {} message(s) failed, rolling back (failure {}/{}): {}",
                            received, failures, MAX_CONSECUTIVE_FAILURES, causeChain(e));
                    if (s.verbose) {
                        log.error("Batch failure details", e);
                    }
                    rollbackQuietly(log, session);
                    if (failures >= MAX_CONSECUTIVE_FAILURES) {
                        ok = false;
                        break;
                    }
                    Thread.sleep(Math.min(1000L * failures, 5000L));
                    continue;
                }

                failures = 0;
                batches++;
                forwarded += sent;
                expired += received - sent;
                lastActivity = System.nanoTime();
                if (s.verbose) {
                    log.debug("Batch #{}: committed {} message(s), {} expired/dropped", batches, sent, received - sent);
                }
            }

            if (ok && s.once && forwarded == 0) {
                log.error("No message received within {} ms", s.timeoutMs);
                ok = false;
            }

        } catch (Exception e) {
            if (s.verbose) {
                log.error("Forwarder failed", e);
            } else {
                log.error("Forwarder failed: {} (run with -v for details)", causeChain(e));
            }
            ok = false;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    log.warn("Error closing connection: {}", e.getMessage());
                }
            }
        }

        long elapsed = System.nanoTime() - start;
        log.info("Forwarded {} messages in {} batches ({} expired and dropped), {} msg/s",
                forwarded, batches, expired, rate(forwarded, elapsed));
        return ok;
    }

    /**
     * Sends the received message to queueOut as is. Nothing is unmarshalled, so the cost per message is tiny.
     * Delivery mode and priority are kept; the TTL becomes the time that is left.
     *
     * @return false if the message had already expired and was therefore not forwarded
     */
    private static boolean forward(MessageProducer producer, Message m) throws JMSException {
        long ttl = 0;
        long expiration = m.getJMSExpiration();
        if (expiration > 0) {
            ttl = expiration - System.currentTimeMillis();
            if (ttl <= 0) {
                return false;
            }
        }
        producer.send(m, m.getJMSDeliveryMode(), m.getJMSPriority(), ttl);
        return true;
    }

    private static void rollbackQuietly(Logger log, Session session) {
        try {
            session.rollback(); // messages go back to queueIn and are redelivered; nothing reaches queueOut
        } catch (JMSException e) {
            log.warn("Rollback failed: {}", e.getMessage());
        }
    }

    private static long rate(long messages, long elapsedNanos) {
        return elapsedNanos <= 0 ? 0 : messages * 1_000_000_000L / elapsedNanos;
    }

    // ---------------------------------------------------------------- logging / verbose

    private static void configureLogging(boolean verbose) {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", String.valueOf(verbose));
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", verbose ? "debug" : "info");

        if (verbose) {
            // JSSE debug output (goes to stderr, same as the slf4j output). Works as long as it's set
            // before the first SSL class is loaded - if you don't see it, pass it with -D on the command line.
            System.setProperty("javax.net.debug", "ssl:handshake:verbose:trustmanager");
        }
    }

    /** Adds ?trace=true so ActiveMQ's TransportLogger dumps every OpenWire command at DEBUG level. */
    private static String withTransportTrace(Logger log, String url) {
        if (url.contains("trace=")) {
            return url;
        }
        if (url.startsWith("failover:")) {
            log.debug("failover: URL - add '?trace=true' to the nested ssl:// URIs yourself if you want wire tracing");
            return url;
        }
        return url + (url.contains("?") ? "&" : "?") + "trace=true";
    }

    private static void logDnsResolution(Logger log, String url) {
        Matcher m = Pattern.compile("(?:nio\\+ssl|ssl|nio|tcp)://([^:/?,)]+)").matcher(url);
        while (m.find()) {
            String host = m.group(1);
            try {
                for (InetAddress a : InetAddress.getAllByName(host)) {
                    log.debug("DNS {} -> {}", host, a.getHostAddress());
                }
            } catch (UnknownHostException e) {
                log.warn("DNS lookup failed for {}: {}", host, e.getMessage());
            }
        }
    }

    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (sb.length() > 0) {
                sb.append(" <- ");
            }
            sb.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage());
            if (c.getCause() == c) {
                break;
            }
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------- tiny arg parsing

    private static boolean has(String[] args, String flag) {
        for (String a : args) {
            if (a.equals(flag)) {
                return true;
            }
        }
        return false;
    }

    private static String opt(String[] args, String name, String envName, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        String env = System.getenv(envName);
        return env != null && !env.isEmpty() ? env : def;
    }

    private static void usage() {
        System.err.println(
                "Usage: java -jar mq-test.jar [-v] --url <brokerUrl> --user <user> --password <pass>\n"
                        + "                             [--queue-in <name>] [--queue-out <name>] [--batch-size <n>]\n"
                        + "                             [--prefetch <n>] [--timeout <ms>] [--once]\n"
                        + "\n"
                        + "Forwards every message from queue-in to queue-out in batches: one transaction per batch,\n"
                        + "the commit acknowledges the consumed messages and publishes the forwarded ones atomically.\n"
                        + "\n"
                        + "  -v, --verbose   DEBUG logs (ActiveMQ, wire trace), JSSE handshake/SSL debug, DNS info\n"
                        + "  --url           e.g. ssl://b-xxxx-1.mq.eu-central-1.amazonaws.com:61617\n"
                        + "                  or   failover:(ssl://b-xxxx-1.mq...:61617,ssl://b-xxxx-2.mq...:61617)\n"
                        + "  --user          broker user (not an IAM user)\n"
                        + "  --password      broker password (or env MQ_PASSWORD, keeps it out of 'ps')\n"
                        + "  --queue-in      queue to consume from, default: mq-test-queue-in\n"
                        + "  --queue-out     queue to forward to, default: mq-test-queue-out\n"
                        + "  --batch-size    max messages per transaction, default 500\n"
                        + "  --prefetch      consumer prefetch, default = batch-size\n"
                        + "  --timeout       with --once: exit after queue-in was idle this long (ms), default 10000\n"
                        + "  --once          drain queue-in and exit instead of running until Ctrl+C\n");
    }
}