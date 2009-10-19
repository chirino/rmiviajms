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

import javax.jms.*;

import org.fusesource.rmiviajms.internal.JMSTemplate.TemplateClosedException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.io.NotSerializableException;
import java.rmi.RemoteException;
import java.rmi.MarshalException;
import java.rmi.ServerException;
import java.rmi.ServerError;

/**
 * @author chirino
 */
final class RequestExchange implements Runnable {

    private final JMSRemoteRef remoteRef;
    private final boolean oneway;
    private final long timeout;
    private final int deliveryMode;
    private final int priority;
    private final Request request;
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final CountDownLatch completed = new CountDownLatch(1);
    private final AtomicReference<Response> response = new AtomicReference<Response>();
    private JMSRemoteSystem remoteSystem;

    public RequestExchange(JMSRemoteSystem remoteSystem, JMSRemoteRef remoteRef, String signature, Object[] params, boolean oneway, long timeout, int deliveryMode, int priority) {
        this.remoteSystem = remoteSystem;
        this.remoteRef = remoteRef;
        this.oneway = oneway;
        this.timeout = timeout;
        this.deliveryMode = deliveryMode;
        this.priority = priority;
        this.request = new Request(remoteRef.getObjectId(), signature, params, remoteSystem.requestCounter.incrementAndGet());
    }

    public Object getResult() throws Throwable {
        if (timeout > 0) {
            if (!completed.await(timeout, TimeUnit.MILLISECONDS)) {
                canceled.set(true);
                throw new RemoteException("request tmeout");
            }
        } else {
            completed.await();
        }

        Response r = response.get();
        if (r.exception != null) {
            if (r.fromRemote) {
                // We may need to wrap the exceptions a bit..
                if (r.exception instanceof RemoteException) {
                    throw new ServerException(r.exception.toString(), (RemoteException) r.exception);
                } else if (r.exception instanceof Error) {
                    throw new ServerError(r.exception.toString(), (Error) r.exception);
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
        if (canceled.get())
            return;

        ObjectMessage msg = null;
        if (!oneway) {
            remoteSystem.requests.put(request.requestId, this);
        }
        try {
            while (!canceled.get() && remoteSystem.running.get()) {
                try {
                    if (msg == null) {
                        // To go faster most JMS providers should let use do the following
                        // in original thread.. but to stay true to the spec, we are doing
                        // it in the sending thread to avoid multi-threaded session access.
                        Session session = remoteSystem.sendTemplate.getSession();
                        msg = session.createObjectMessage();

                        try {
                            msg.setObject(request);
                            msg.setLongProperty(JMSRemoteSystem.MSG_PROP_OBJECT, request.objectId);
                            if (oneway) {
                                msg.setJMSType(JMSRemoteSystem.MSG_TYPE_ONEWAY);
                            } else {
                                msg.setJMSType(JMSRemoteSystem.MSG_TYPE_REQUEST);
                                //Set the request id in the properties, so that error response can be returned
                                //if there is an error unmarshalling the request at the other end:
                                msg.setLongProperty(JMSRemoteSystem.MSG_PROP_REQUEST, request.requestId);
                                msg.setJMSReplyTo(remoteSystem.sendTemplate.getLocalSystemQueue());
                            }
                        } catch (JMSException e) {
                            throw new MarshalException("Could not marshall request: " + e.getMessage(), e);
                        }
                    }

                    Destination destination = remoteRef.getDestination();
                    MessageProducer producer = remoteSystem.sendTemplate.getMessageProducer();

                    producer.send(destination, msg, deliveryMode, priority, timeout);
                    return;

                } catch (RemoteException e) {
                    setResponse(new Response(request.requestId, null, e));
                    return;
                } catch (TemplateClosedException tce) {
                    setResponse(new Response(request.requestId, null, tce));
                    return;
                } catch (Exception e) {
                    Throwable cause = e.getCause();
                    if (cause != null && cause instanceof NotSerializableException) {
                        setResponse(new Response(request.requestId, null, e.getCause()));
                        return;
                    }
                    e.printStackTrace();
                    remoteSystem.sendTemplate.reset();
                    // TODO: should we sleep?
                }
            }
        } finally {
            if (oneway) {
                // Lests the calling thread continue.. (since it won't be getting a response).
                setResponse(new Response(0, null, null));
            }
        }
        if (canceled.get()) {
            if (!oneway) {
                remoteSystem.requests.remove(request.requestId);
            }
        }

    }
}