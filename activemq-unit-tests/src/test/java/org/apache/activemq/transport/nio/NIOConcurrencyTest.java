/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.nio;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.ConsumerThread;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({ "javadoc" })
public class NIOConcurrencyTest extends TestCase {

    BrokerService broker;
    Connection connection;

    public static final int PRODUCER_COUNT = 6;
    public static final int CONSUMER_COUNT = 6;
    public static final int MESSAGE_COUNT = 8000;
    public static final int MESSAGE_SIZE = 4096;

    LinkedList<ConsumerThread> consumers = new LinkedList<ConsumerThread>();

    byte[] messageData;
    AtomicBoolean failed = new AtomicBoolean();
    String brokerUrl;

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        failed.set(false);
        messageData = new byte[MESSAGE_SIZE];
        for (int i = 0; i < MESSAGE_SIZE;  i++)
        {
            messageData[i] = (byte) (i & 0xff);
        }

        brokerUrl = "tcp://localhost:" + connector.getConnectUri().getPort();
    }

    @Override
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    public void testLoad() throws Exception {
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            ProducerThread producer = new ProducerThread("P-" + i, new ActiveMQQueue("TEST" + i));
            producer.setMessageCount(MESSAGE_COUNT);
            producer.start();
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            ConsumerThread consumer = new ConsumerThread("C-" + i, new ActiveMQQueue("TEST" + i));
            consumer.setMessageCount(MESSAGE_COUNT);
            consumer.start();
            consumers.add(consumer);
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return failed.get() || getReceived() == PRODUCER_COUNT * MESSAGE_COUNT;
            }
        }, 10*60000);

        assertFalse(failed.get());
        assertEquals(PRODUCER_COUNT * MESSAGE_COUNT, getReceived());

    }

    protected int getReceived() {
        int received = 0;
        for (ConsumerThread consumer : consumers) {
            received += consumer.getReceived();
        }
        return received;
    }

    private class ConsumerThread extends Thread {

        private final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

        int messageCount = 0;
        int received = 0;
        Destination dest;
        boolean breakOnNull = true;

        public ConsumerThread(String name, Destination dest) {
            super(name);
            this.dest = dest;
        }

        @Override
        public void run() {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            MessageConsumer consumer = null;
            ActiveMQConnection conn = null;
            try {
                while (received < messageCount) {

                    conn = (ActiveMQConnection)factory.createConnection();
                    Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    conn.start();

                    consumer = sess.createConsumer(dest);

                    Message msg = consumer.receive(20000);
                    if (msg == null) {
                        msg = consumer.receive(5000);
                    }

                    conn.close();
                    if (msg != null) {
                        received++;
                        //LOG.info("Received test message: " + received++);
                    } else {
                        if (breakOnNull) {
                            LOG.info("Giving up on null at: " + received);
                            break;
                        }
                    }
                }
                LOG.info("Done, consume: " + received);

            } catch (JMSException e) {
                if (failed.compareAndSet(false, true)) {
                    LOG.error("conn " + conn.getTransport() + ", error", e);
                    e.printStackTrace();
                    TestSupport.dumpAllThreads("consumer");
                }
            }
        }

        public int getReceived() {
            return received;
        }

        public void setMessageCount(int messageCount) {
            this.messageCount = messageCount;
        }
    }

    private class ProducerThread extends Thread {

        private final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

        int messageCount = 1000;
        Destination dest;
        int sentCount = 0;

        public ProducerThread(String name, Destination dest) {
            super(name);
            this.dest = dest;
        }

        @Override
        public void run() {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            MessageProducer producer = null;
            ActiveMQConnection conn = null;
            try {
                for (sentCount = 0; sentCount < messageCount; sentCount++) {
                    conn = (ActiveMQConnection) factory.createConnection();
                    Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    conn.start();
                    producer = sess.createProducer(dest);
                    producer.send(createMessage(sess));
                    conn.close();
                }
                LOG.info("Done, sent: " + sentCount);

            } catch (Exception e) {
                if (failed.compareAndSet(false, true)) {
                    e.printStackTrace();
                    LOG.error("conn " + conn.getTransport() + ", error", e);

                    TestSupport.dumpAllThreads("producer-" +sentCount);
                }
            }
        }

        protected Message createMessage(Session sess) throws Exception {
            BytesMessage b = sess.createBytesMessage();
            b.writeBytes(messageData);
            return b;
        }

        public void setMessageCount(int messageCount) {
            this.messageCount = messageCount;
        }

    }
}
