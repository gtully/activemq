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

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.demo.DefaultQueueSender;

/**
 * A helper class which can be handy for running a broker in your IDE from the
 * activemq-core module.
 * 
 * 
 */
public final class Main {
    protected static boolean createConsumers;

    private Main() {        
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            String brokerUrl = "xbean:/Users/gtully/code/activemq.gits/assembly/src/release/examples/conf/activemq-specjms.xml";
            System.setProperty("activemq.data", "/Users/gtully/code/activemq.gits/activemq-unit-tests/target");
            BrokerService broker = BrokerFactory.createBroker(brokerUrl);
            broker.start();
            // Lets wait for the broker
            broker.waitUntilStopped();
        } catch (Exception e) {
            System.out.println("Failed: " + e);
            e.printStackTrace();
        }
    }
}
