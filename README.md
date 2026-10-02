This is an example of sending and receiving a JMS1.1 message via activemq-client on JDK8 with the most recent client driver.

Minimal deps, fat jar.

install:

```
mvn clean package
```

download and run broker:

```
curl -fL https://archive.apache.org/dist/activemq/5.19.11/apache-activemq-5.19.11-bin.tar.gz | tar xz && JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto apache-activemq-5.19.11/bin/activemq console
```

_activemq-5.19 could run on jdk-11 and above, so correct your): $JAVA_HOME according to some new jdk_


run the test:

```
java -jar targert/test-active-mq-jdk8.jar --url tcp://localhost:61616 --user admin --password admin
```

verbose logging, including ssl handshakes:

```
java -jar targert/test-active-mq-jdk8.jar --url tcp://localhost:61616 --user admin --password admin -v
```
