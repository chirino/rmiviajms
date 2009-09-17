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

import javax.jms.*;

/**
     * Helper JMS class that caches the JMS objects.
 */
class JMSTemplate {
    private Session session;
    private Connection connection;
    private ConnectionFactory connectionFactory;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private Destination localSystemQueue;
    private JMSRemoteSystem remoteSystem;

    public JMSTemplate(JMSRemoteSystem remoteSystem) {
        this.remoteSystem = remoteSystem;
    }

    void reset() {
        try {
            connection.close();
        } catch(Throwable ignore) {
        }
        connection = null;
        session= null;
        producer=null;
        consumer=null;
        connectionFactory=null;
        localSystemQueue =null;
    }

    Destination getLocalSystemQueue() {
        if( localSystemQueue ==null ) {
            localSystemQueue = remoteSystem.createQueue(remoteSystem.getSystemId());
        }
        return localSystemQueue;
    }

    MessageConsumer getMessageConsumer(Destination destination) throws JMSException {
        if( consumer==null) {
            consumer = getSession().createConsumer(destination);
        }
        return consumer;
    }

    MessageConsumer getMessageConsumer() throws JMSException {
        return getMessageConsumer(getLocalSystemQueue());
    }

    MessageProducer getMessageProducer() throws JMSException {
        if(producer==null) {
            producer = getSession().createProducer(null);
        }
        return producer;
    }

    Session getSession() throws JMSException {
        if(session==null) {
            session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        return session;
    }

    Connection getConnection() throws JMSException {
        if(connection==null) {
            connection = getConnectionFactory().createConnection();
            connection.setExceptionListener(new ExceptionListener(){
                public void onException(JMSException exception) {
                    reset();
                }
            });
            connection.start();
        }
        return connection;
    }

    ConnectionFactory getConnectionFactory() throws JMSException  {
        if(connectionFactory==null) {
            connectionFactory = remoteSystem.createConnectionFactory();
        }
        return connectionFactory;
    }

}