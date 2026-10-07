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
import javax.jms.TextMessage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sends one message to a queue on Amazon MQ (ActiveMQ, OpenWire over SSL) and receives it back.
 *
 * Usage:
 *   java -jar mq-test.jar [-v] --url <brokerUrl> --user <user> --password <pass> [--queue <name>] [--timeout <ms>]
 *
 * Every option can also come from an env var: MQ_URL, MQ_USER, MQ_PASSWORD, MQ_QUEUE, MQ_TIMEOUT_MS.
 *
 * Exit code: 0 = message round trip OK, 1 = failed, 2 = bad arguments.
 */

public class SendReceive {
	public static void main(String[] args) {
        if (has(args, "-h") || has(args, "--help")) {
            usage();
            return;
        }

        boolean verbose = has(args, "-v") || has(args, "--verbose");
        String url = opt(args, "--url", "MQ_URL", null);
        String user = opt(args, "--user", "MQ_USER", null);
        String password = opt(args, "--password", "MQ_PASSWORD", null);
        String queueName = opt(args, "--queue", "MQ_QUEUE", "mq-test-queue");
        long timeoutMs = Long.parseLong(opt(args, "--timeout", "MQ_TIMEOUT_MS", "10000"));

        if (url == null || user == null || password == null) {
            System.err.println("Missing --url / --user / --password (or MQ_URL / MQ_USER / MQ_PASSWORD)\n");
            usage();
            System.exit(2);
        }

        // Must happen BEFORE the first logger is created and before any SSL class is touched.
        configureLogging(verbose);
        Logger log = LoggerFactory.getLogger(SendReceive.class);

        boolean ok = runTest(log, verbose, url, user, password, queueName, timeoutMs);
        log.info("RESULT: {}", ok ? "PASS" : "FAIL");
        System.exit(ok ? 0 : 1);
    }

    private static boolean runTest(Logger log, boolean verbose, String url, String user, String password,
                                   String queueName, long timeoutMs) {
        String brokerUrl = url;
        if (verbose) {
            log.debug("java.version={} java.vendor={}", System.getProperty("java.version"), System.getProperty("java.vendor"));
            logDnsResolution(log, url);
            brokerUrl = withTransportTrace(log, url);
        }
        log.info("Connecting to {} as '{}' (queue '{}')", brokerUrl, user, queueName);

        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory(brokerUrl);
        factory.setUserName(user);
        factory.setPassword(password);

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.setExceptionListener(e -> log.error("Async connection exception", e));
            connection.start();

            ConnectionMetaData meta = connection.getMetaData();
            log.info("Connected. Provider: {} {}", meta.getJMSProviderName(), meta.getProviderVersion());

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);

            // unique id + selector => we only ever pick up our own message, never leftovers
            String testId = UUID.randomUUID().toString();
            String body = "mq-test hello " + testId;

            MessageConsumer consumer = session.createConsumer(queue, "testId = '" + testId + "'");
            MessageProducer producer = session.createProducer(queue);
            producer.setTimeToLive(60_000); // don't leave garbage behind if the test fails

            TextMessage out = session.createTextMessage(body);
            out.setStringProperty("testId", testId);

            long t0 = System.nanoTime();
            producer.send(out);
            log.info("SENT     id={} body='{}'", out.getJMSMessageID(), body);

            Message in = consumer.receive(timeoutMs);
            long ms = (System.nanoTime() - t0) / 1_000_000;

            if (in == null) {
                log.error("No message received within {} ms", timeoutMs);
                return false;
            }
            if (!(in instanceof TextMessage)) {
                log.error("Received unexpected message type: {}", in.getClass().getName());
                return false;
            }
            String received = ((TextMessage) in).getText();
            log.info("RECEIVED id={} body='{}' (round trip {} ms)", in.getJMSMessageID(), received, ms);

            if (!body.equals(received)) {
                log.error("Body mismatch! expected='{}' got='{}'", body, received);
                return false;
            }
            return true;

        } catch (Exception e) {
            if (verbose) {
                log.error("Test failed", e);
            } else {
                log.error("Test failed: {} (run with -v for details)", causeChain(e));
            }
            return false;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    log.warn("Error closing connection: {}", e.getMessage());
                }
            }
        }
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
                "Usage: java -jar mq-test.jar [-v] --url <brokerUrl> --user <user> --password <pass> [--queue <name>] [--timeout <ms>]\n"
                        + "\n"
                        + "  -v, --verbose   DEBUG logs (ActiveMQ, wire trace), JSSE handshake/SSL debug, DNS info\n"
                        + "  --url           e.g. ssl://b-xxxx-1.mq.eu-central-1.amazonaws.com:61617\n"
                        + "                  or   failover:(ssl://b-xxxx-1.mq...:61617,ssl://b-xxxx-2.mq...:61617)\n"
                        + "  --user          broker user (not an IAM user)\n"
                        + "  --password      broker password (or env MQ_PASSWORD, keeps it out of 'ps')\n"
                        + "  --queue         default: mq-test-queue\n"
                        + "  --timeout       receive timeout in ms, default 10000\n");
    }
}
