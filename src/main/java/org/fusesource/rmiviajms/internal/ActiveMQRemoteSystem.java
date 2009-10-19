/**
 * Copyright (C) 2009 Progress Software, Inc.
 * http://fusesource.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.rmiviajms.internal;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.Destination;
import javax.jms.ConnectionFactory;

/**
 * @author chirino
 */
final public class ActiveMQRemoteSystem extends JMSRemoteSystem {

    final static public String CONNECT_URL_PROPNAME = "org.fusesource.rmiviajms.CONNECT_URL";
    final static public String CONNECT_URL = System.getProperty(CONNECT_URL_PROPNAME,"tcp://localhost:61616");
    final static public String QUEUE_PREFIX = System.getProperty("org.fusesource.rmiviajms.QUEUE_PREFIX","rmiviajms.");

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return new ActiveMQConnectionFactory(CONNECT_URL);
    }

    @Override
    protected Destination createQueue(String queueName) {
        return new ActiveMQQueue(QUEUE_PREFIX+queueName);
    }
    
    @Override
    protected Destination createTopic(String topicName) {
        return new ActiveMQTopic(QUEUE_PREFIX+topicName);
    }

}