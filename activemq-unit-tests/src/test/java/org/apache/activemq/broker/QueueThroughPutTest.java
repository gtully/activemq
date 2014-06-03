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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.*;
import javax.jms.Connection;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.jms.pool.PooledConnection;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueueThroughPutTest {
    private static final Logger LOG = LoggerFactory.getLogger(QueueThroughPutTest.class);
    public static final int payload = 64 * 1024;
    public static final int ioBuffer = 2 * payload;
    public static final int socketBuffer = 8 * ioBuffer;
    private ActiveMQQueue receiveDestination = null;
    private final Destination sendDestination = new ActiveMQQueue("T");

    private final String payloadString = new String(new byte[payload]);
    private final int threads = 4;
    private final int parallelProducer = threads;
    private final int parallelConsumer = threads;
    private final int producerConnections = threads;
    private final int consumerConnections = threads;
    private final boolean asyncDispatch = true;
    private final boolean queuePerConsumer = false;
    private final boolean useVirtualQueueForLoadBalance = false;
    private final Vector<Exception> exceptions = new Vector<Exception>();

    int queuePrefetch = 3000;
    int toSend = 500000;

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private ConnectionFactory producerConnectionFactory;
    private ConnectionFactory consumerConnectionFactory;

    Collection<ActiveMQQueue> routes = new ArrayList<ActiveMQQueue>();
    private final boolean syncSendOnComplete = true;

    String wireFormatOptions = "";/*
            "&wireFormat.tightEncodingEnabled=false"
            // cache size is < 10 entries with connection per producer/consumer
            + "&wireFormat.cacheEnabled=false" // needed for writeThread
            //+ "&wireFormat.cacheSize=888192"
            + "&wireFormat.tcpNoDelayEnabled=true"
            + "&wireFormat.sizePrefixDisabled=false";    */


    @Before
    public void initDest() throws Exception {
        for (int i=0; i<threads; i++) {
            routes.add(new ActiveMQQueue("T." + i));
        }

        if (useVirtualQueueForLoadBalance) {
            receiveDestination = new ActiveMQQueue();
            receiveDestination.setCompositeDestinations(routes.toArray(new ActiveMQQueue[]{}));
        } else {
            receiveDestination = new ActiveMQQueue("T");
        }
    }

   @Test
   public void brokerOnly() throws Exception {
       startBroker();
       broker.waitUntilStopped();
   }

    @Test
    public void testProduceConsume() throws Exception {
        startBroker();
        createConnectionFactory();

        final AtomicLong sharedSendCount = new AtomicLong(toSend);
        final CountDownLatch producersDone = new CountDownLatch(parallelProducer);
        final CountDownLatch sharedConsumerCount = new CountDownLatch(toSend);

        ExecutorService executorService = Executors.newFixedThreadPool(parallelConsumer + parallelProducer);

        for (int i = 0; i < parallelConsumer; i++) {
            final ActiveMQQueue destination = queuePerConsumer ? new ActiveMQQueue("T."+ i) : receiveDestination;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        consumeMessages(sharedConsumerCount, destination);
                    } catch (Exception e) {
                        e.printStackTrace();
                        exceptions.add(e);
                    }
                }
            });
        }

        // let consumers settle
        TimeUnit.SECONDS.sleep(2);

        LOG.info("Starting producers...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < parallelProducer; i++) {
            final Destination destination = queuePerConsumer ? new ActiveMQQueue("T."+ i) : sendDestination;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        publishMessages(sharedSendCount, producersDone, destination);
                    } catch (Exception e) {
                        e.printStackTrace();
                        exceptions.add(e);
                    }
                }
            });
        }
        if (parallelProducer > 0) {
            producersDone.await();
        }
        if (parallelConsumer > 0) {
            sharedConsumerCount.await();
        }
        double duration = System.currentTimeMillis() - start;

        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.MINUTES);
        assertTrue("Producers done in time", executorService.isTerminated());
        assertTrue("No exceptions: " + exceptions, exceptions.isEmpty());

        stopBroker();
        LOG.info("Duration:                " + duration + "ms");
        LOG.info("Rate:                    " + (toSend * 1000 / duration) + "m/s");
    }

    private void consumeMessages(final CountDownLatch latch, Destination destination) throws Exception {
        Connection connection = consumerConnectionFactory.createConnection();
        connection.start();
        LOG.error("Consumer connection:" + connection);
        Session session = connection.createSession(false, ActiveMQSession.BROKER_DISPATCH_ACKNOWLEDGE);
        //Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            int count;
            @Override
            public void onMessage(Message message) {
                latch.countDown();
                if (++count % 100000 == 0) {
                    LOG.info(consumer + " got: " + count);
                }
            }
        });
        latch.await();
        connection.close();
    }

    private void publishMessages(AtomicLong count, CountDownLatch producersDone, Destination destination) throws Exception {
        Connection connection = producerConnectionFactory.createConnection();
        connection.start();
        LOG.error("Producer connection:" + connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        Message message = session.createBytesMessage();
        ((BytesMessage) message).writeBytes(payloadString.getBytes());

        while (count.getAndDecrement() > 0) {
            producer.send(message);
        }
        producersDone.countDown();

        if (syncSendOnComplete) {
            ActiveMQConnection activeMQConnection = null;

            if (connection instanceof PooledConnection) {
                activeMQConnection =  (ActiveMQConnection) ((PooledConnection)connection).getConnection();
            } else {
                activeMQConnection = (ActiveMQConnection) connection;
            }
            activeMQConnection.syncSendPacket(new ConnectionControl());
        }
        connection.close();
    }

    public void startBroker() throws Exception {
        broker = new BrokerService();

        broker.setPersistent(false);
        broker.setAdvisorySupport(false);
        broker.setUseJmx(false);
        broker.setEnableStatistics(false);
        broker.setSchedulerSupport(false);
        broker.setUseMirroredQueues(false);
        broker.setUseVirtualTopics(false);
        broker.getSystemUsage().getMemoryUsage().setPercentOfJvmHeap(80);
        broker.addConnector("tcp://0.0.0.0:61616?transport.useInactivityMonitor=false&transport.socketBufferSize=" + socketBuffer + "&transport.ioBufferSize=" + ioBuffer + wireFormatOptions);


        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policyEntry.setPrioritizedMessages(false);
        policyEntry.setExpireMessagesPeriod(0);
        policyEntry.setEnableAudit(false);

        policyEntry.setOptimizedDispatch(true);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);


        if (useVirtualQueueForLoadBalance) {
            CompositeQueue route = new CompositeQueue();
            route.setName("VirtualQ");
            route.setForwardOnly(true);
            route.setForwardTo(routes);
            VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
            interceptor.setVirtualDestinations(new VirtualDestination[]{route});
            broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        }

        broker.start();
    }

    public void createConnectionFactory() throws Exception {
        String options = "?useInactivityMonitor=false&jms.watchTopicAdvisories=false&jms.useAsyncSend=true&jms.alwaysSessionAsync=false&jms.dispatchAsync="+ asyncDispatch
                + "&jms.prefetchPolicy.queuePrefetch=" + queuePrefetch
                + "&jms.copyMessageOnSend=false&jms.messagePrioritySupported=false&socketBufferSize=" + socketBuffer
                + "&ioBufferSize=" + ioBuffer
                + wireFormatOptions;
        connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616" + options); //broker.getTransportConnectors().get(0).getConnectUri() + options);
        connectionFactory.setWatchTopicAdvisories(false);

        producerConnectionFactory = createPool(connectionFactory, producerConnections);
        consumerConnectionFactory = createPool(connectionFactory, consumerConnections);

    }

    private ConnectionFactory createPool(final ConnectionFactory factory, final int numConnections) {
        return new ConnectionFactory() {

            int index = 0;
            Connection[] connections = new Connection[numConnections];

            public void init() {
                if (connections[0] == null) {
                    for (int i=0;i<numConnections; i++) {
                        try {
                            connections[i] = factory.createConnection();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            @Override
            public synchronized Connection createConnection() throws JMSException {
                init();
                return connections[(index++)%connections.length];
            }

            @Override
            public  Connection createConnection(String userName, String password) throws JMSException {
                return createConnection();
            }
        };
        // issues with pool connection reuse in error when consumer count == maxConnectionCount
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

}