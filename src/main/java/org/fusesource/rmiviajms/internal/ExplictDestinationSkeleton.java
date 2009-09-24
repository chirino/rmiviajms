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

import org.fusesource.rmiviajms.internal.JMSTemplate.TemplateClosedException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.rmi.Remote;

/**
 * @author chirino
*/
class ExplictDestinationSkeleton extends Skeleton implements Runnable {

    private JMSRemoteSystem remoteSystem;
    private final JMSTemplate template;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final JMSRemoteRef ref;
    Thread receiveThread;

    ExplictDestinationSkeleton(JMSRemoteSystem remoteSystem, JMSRemoteRef ref, Object target) {
        super(remoteSystem, ref, target);
        this.remoteSystem = remoteSystem;
        this.template = new JMSTemplate(remoteSystem);
        this.ref = ref;
    }

    public void start() throws JMSException, TemplateClosedException {
        if( receiveThread == null ) {
            running.set(true);
            //Create the consumer so we don't miss messages:
            template.getMessageConsumer(ref.getDestination());
            receiveThread = new Thread(this);
            receiveThread.setName("RMI via JMS: receiver");
            receiveThread.setDaemon(true);
            receiveThread.start();
        }
    }

    @Override
    public Response invoke(Request request) {
        return super.invoke(request);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public void stop() throws InterruptedException {
        if( receiveThread != null ) {
            running.set(false);
            receiveThread.join();
            receiveThread=null;
        }
    }

    public void run() {
        while( running.get() ) {
            try {
                Session session = template.getSession();
                MessageConsumer consumer = template.getMessageConsumer(ref.getDestination());
                Message msg = consumer.receive(500);
                
                if( msg!=null ) {
                    if( JMSRemoteSystem.MSG_TYPE_REQUEST.equals(msg.getJMSType()) ) {
                        // Handle decoding the message in the dispatch thread.
                        remoteSystem.getDispatchThreads().execute(new DispatchTask((ObjectMessage)msg, false));
                    } else if( JMSRemoteSystem.MSG_TYPE_ONEWAY.equals(msg.getJMSType()) ) {
                        // Handle decoding the message in the dispatch thread.
                        remoteSystem.getDispatchThreads().execute(new DispatchTask((ObjectMessage)msg, true));
                    }

                }
            } 
            catch (TemplateClosedException tce) {
                //TODO we should probably just eat this.
                tce.printStackTrace();
                return;
            } 
            catch (Throwable e) {
                e.printStackTrace();
                template.reset();
            }
        }
    }

    /**
     * This task demarshalls an received message, invokes the exported object
     * and sends the response via the sender thread.
     */
    private class DispatchTask implements Runnable {
        private final ObjectMessage msg;
        private final boolean oneway;

        public DispatchTask(ObjectMessage msg, boolean oneway) {
            this.msg = msg;
            this.oneway = oneway;
        }

        public void run() {
            try {
                Thread.currentThread().setContextClassLoader(getTargetClassLoader());
                Request request = (Request)(msg).getObject();
                Response response = invoke(request);
                if ( !oneway ) {
                    remoteSystem.sendResponse(msg, response);
                }
            } catch (JMSException e) {
                // The request message must not have been properly created.. ignore for now.
            }
        }
    }
}