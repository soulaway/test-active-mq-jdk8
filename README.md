##This is an example of sending and receiving a JMS1.1 message via activemq-client on JDK8 with the most recent client driver.

Minimal deps, fat jar.

###SendReceive mode

Sends one message to Amazon MQ (or apache ActiveMQ, OpenWire over SSL) and receives it back. Usage: `java -jar mq-test.jar [-v] --url --user --password [--queue ] [--timeout ]` Every option can also come from an env var: `MQ_URL, MQ_USER, MQ_PASSWORD, MQ_QUEUE, MQ_TIMEOUT_MS`. 

*Exit code: 0 = message round trip OK, 1 = failed, 2 = bad arguments.*

```|install
mvn clean package -Psend-receive
```

###Forwarder mode

Forwards messages from queueIn to queueOut on Amazon MQ (ActiveMQ, OpenWire over SSL). 

How it works: 

- ONE transacted session is used for both the consumer and the producer.
- The forwarder blocks until a message shows up in queueIn, then drains everything that is already available (up to --batch-size, served from the consumer's prefetch buffer, no extra round trips), sends every message to queueOut and finally calls session.commit() ONCE per batch. 
- commit() is the acknowledgement: the consumed messages are acked and the forwarded copies are published atomically. If anything fails the batch is rolled back, so nothing is lost (the messages stay in queueIn and are redelivered) and nothing is half-forwarded. 
- Sends inside a transaction are asynchronous, so the whole batch is pipelined to the broker and the only synchronous wait is the commit. 
- Messages are forwarded as received (the body is not unmarshalled); delivery mode, priority and the remaining time-to-live are preserved. 
- Messages that already expired are consumed and dropped. 

Usage: 

`java -jar mq-test.jar [-v] --url --user --password [--queue-in ] [--queue-out ] [--batch-size ] [--prefetch ] [--timeout ] [--once]` Every option can also come from an env var: `MQ_URL, MQ_USER, MQ_PASSWORD, MQ_QUEUE_IN, MQ_QUEUE_OUT, MQ_BATCH_SIZE, MQ_PREFETCH, MQ_TIMEOUT_MS`. Runs until Ctrl+C / SIGTERM (the batch in flight is finished and committed first), or with --once until queueIn has been idle for --timeout ms. 

*Exit code: 0 = OK (with --once: at least one message forwarded), 1 = failed, 2 = bad arguments.*

```|install
mvn clean package -Pforwarder
```

###Setup env. - download and run broker:

```
curl -fL https://archive.apache.org/dist/activemq/5.19.11/apache-activemq-5.19.11-bin.tar.gz | tar xz && JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto apache-activemq-5.19.11/bin/activemq console
```

_activemq-5.19 could run on jdk-11 and above, so correct your): $JAVA_HOME according to some new jdk_


###Run the test:

####run tool:

```
java -jar targert/test-active-mq-jdk8.jar --url tcp://localhost:61616 --user admin --password admin
```

verbose logging, including ssl handshakes:

```
java -jar targert/test-active-mq-jdk8.jar --url tcp://localhost:61616 --user admin --password admin -v
```

#### forwarder:

send single message:

```
curl -s -u admin:admin -H "Origin: http://localhost" -H "Content-Type: application/json" -d '{"type":"exec","mbean":"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=mq-test-queue-in","operation":"sendTextMessage(java.lang.String)","arguments":["hello"]}' http://127.0.0.1:8161/api/jolokia/
```

send 1k bulk

```
for i in $(seq 1 1000); do curl -s -u admin:admin -H "Origin: http://localhost" -H "Content-Type: application/json" -d "{\"type\":\"exec\",\"mbean\":\"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=mq-test-queue-in\",\"operation\":\"sendTextMessage(java.lang.String)\",\"arguments\":[\"msg-$i\"]}" http://127.0.0.1:8161/api/jolokia/; echo; done | grep -c '"status":200'
```