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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.rmi.RemoteException;
import java.rmi.MarshalException;
import java.rmi.ServerException;
import java.rmi.ServerError;

/**
 * @author chirino
*/
final class RequestExchange implements Runnable {

    private final JMSRemoteRef remoteRef;
    private final Request request;
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final CountDownLatch completed = new CountDownLatch(1);
    private final AtomicReference<Response> response = new AtomicReference<Response>();
    private JMSRemoteSystem remoteSystem;

    public RequestExchange(JMSRemoteSystem remoteSystem, JMSRemoteRef remoteRef, String signature, Object[] params) {
        this.remoteSystem = remoteSystem;
        this.remoteRef = remoteRef;
        this.request = new Request(remoteRef.getObjectId(), signature, params, remoteSystem.requestCounter.incrementAndGet());
    }

    public Object getResult(long timeout, TimeUnit unit) throws Throwable {
        if( !completed.await(timeout, unit) ) {
            canceled.set(true);
            throw new RemoteException("request tmeout");
        }
        Response r = response.get();
        if( r.exception!=null ) {
            if( r.fromRemote ) {
                // We may need to wrap the exceptions a bit..
                if( r.exception instanceof RemoteException) {
                    throw new ServerException(r.exception.toString(), (RemoteException)r.exception);
                } else if( r.exception instanceof Error ) {
                    throw new ServerError(r.exception.toString(), (Error)r.exception);
                }
            }
            throw r.exception;
        }
        return r.result;
    }

    public void setResponse(Response response) {
        this.response.compareAndSet(null, response);
        this.completed.countDown();
    }


    public void cancel() {
        canceled.set(true);
        remoteSystem.requests.remove(request.requestId);
    }

    public void run() {
        if( canceled.get() )
            return;

        ObjectMessage msg=null;
        remoteSystem.requests.put(request.requestId, this);
        while( !canceled.get() && remoteSystem.running.get() ) {
            try {
                Session session = remoteSystem.sendTemplate.getSession();
                MessageProducer producer = remoteSystem.sendTemplate.getMessageProducer();
                if( msg==null ) {
                    try {
                        msg = session.createObjectMessage(request);
                    } catch (JMSException e) {
                        throw new MarshalException("Could not marshall request: "+e.getMessage(), e);
                    }
                }
                
                Destination destination = remoteRef.getDestination();
                msg.setLongProperty(JMSRemoteSystem.MSG_PROP_OBJECT, request.objectId);
                msg.setJMSType(JMSRemoteSystem.MSG_TYPE_REQUEST);
                msg.setJMSReplyTo(remoteSystem.sendTemplate.getLocalSystemQueue());
                producer.send(destination, msg, DeliveryMode.NON_PERSISTENT, 4, 0);
                return;

            } catch ( RemoteException e ) {
                setResponse(new Response(request.requestId, null, e));
                return;
            } catch ( Exception e ) {
                e.printStackTrace();
                remoteSystem.sendTemplate.reset();
                // TODO: should we sleep?
            }
        }
        if ( canceled.get() ) {
            remoteSystem.requests.remove(request.requestId);
        }

    }
}