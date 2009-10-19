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

import java.util.concurrent.atomic.AtomicBoolean;

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
    private AtomicBoolean closed = new AtomicBoolean(false);

    public class TemplateClosedException extends Exception {
    }

    public JMSTemplate(JMSRemoteSystem remoteSystem) {
        this.remoteSystem = remoteSystem;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            reset();
        }
    }

    void reset() {

        Connection oldConn = null;

        synchronized (this) {
            oldConn = connection;
            connection = null;

            if (oldConn != null) {
                connection = null;
                session = null;
                producer = null;
                consumer = null;
                connectionFactory = null;
                localSystemQueue = null;
            }
        }

        if (oldConn != null) {
            try {
                oldConn.close();
            } catch (JMSException jmse) {
            }
        }
    }

    Destination getLocalSystemQueue() {
        if (localSystemQueue == null) {
            localSystemQueue = remoteSystem.createQueue(remoteSystem.getSystemId());
        }
        return localSystemQueue;
    }

    MessageConsumer getMessageConsumer(Destination destination) throws JMSException, TemplateClosedException {
        if (consumer == null) {
            consumer = getSession().createConsumer(destination);
        }
        return consumer;
    }

    MessageConsumer getMessageConsumer() throws JMSException, TemplateClosedException {
        return getMessageConsumer(getLocalSystemQueue());
    }

    MessageProducer getMessageProducer() throws JMSException, TemplateClosedException {
        if (producer == null) {
            producer = getSession().createProducer(null);
        }
        return producer;
    }

    Session getSession() throws JMSException, TemplateClosedException {
        if (session == null) {
            session = getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        return session;
    }

    synchronized Connection getConnection() throws JMSException, TemplateClosedException {
        if (closed.get()) {
            throw new JMSException("JMSTemplate Closed");
        }
        if (connection == null) {
            connection = getConnectionFactory().createConnection();
            connection.setExceptionListener(new ExceptionListener() {
                final Connection thisConn = connection;

                public void onException(JMSException exception) {
                    try {

                        boolean reset = false;
                        synchronized (this) {
                            if (connection == this.thisConn) {
                                reset = true;
                            }
                        }
                        if (reset) {
                            if (!closed.get() && reset) {
                                reset();
                            }
                        }
                    } catch (Throwable thrown) {
                        thrown.printStackTrace();
                    }
                }
            });
            connection.start();
        }
        return connection;
    }

    ConnectionFactory getConnectionFactory() throws JMSException {
        if (connectionFactory == null) {
            connectionFactory = remoteSystem.createConnectionFactory();
        }
        return connectionFactory;
    }

}