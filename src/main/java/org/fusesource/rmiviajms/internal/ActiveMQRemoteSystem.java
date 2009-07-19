package org.fusesource.rmiviajms.internal;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.Destination;
import javax.jms.ConnectionFactory;

/**
 * @author chirino
 */
final public class ActiveMQRemoteSystem extends JMSRemoteSystem {

    final static public String CONNECT_URL = System.getProperty("org.fusesource.rmiviajms.CONNECT_URL","tcp://localhost:61616");
    final static public String QUEUE_PREFIX = System.getProperty("org.fusesource.rmiviajms.QUEUE_PREFIX","rmiviajms.");

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return new ActiveMQConnectionFactory(CONNECT_URL);
    }

    @Override
    protected Destination createQueue(String systemId) {
        return new ActiveMQQueue(QUEUE_PREFIX+systemId);
    }

}