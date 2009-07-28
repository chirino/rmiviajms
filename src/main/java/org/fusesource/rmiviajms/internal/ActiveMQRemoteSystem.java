/**
 * ====================================================================
 * Copyright (C) 2008 Progress Software, Inc. All rights reserved.
 * http://fusesource.com
 * ====================================================================
 * The software in this package is published under the terms of 
 * the AGPL license a copy of which has been included with this 
 * distribution in the license.txt file.
 * ====================================================================
 */
package org.fusesource.rmiviajms.internal;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

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
    protected Destination createQueue(String systemId) {
        return new ActiveMQQueue(QUEUE_PREFIX+systemId);
    }

}