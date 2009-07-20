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
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.server.ExportException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chirino
 */
public abstract class JMSRemoteSystem {

    public static final String REMOTE_SYSTEM_CLASS = System.getProperty("org.fusesource.rmiviajms.REMOTE_SYSTEM_CLASS", "org.fusesource.rmiviajms.internal.ActiveMQRemoteSystem");
    public static final long REQUEST_TIMEOUT = new Long(System.getProperty("org.fusesource.rmiviajms.REQUEST_TIMEOUT", ""+Long.MAX_VALUE));

    protected static final String MSG_TYPE_REQUEST = "rmi:request";
    protected static final String MSG_TYPE_RESPONSE = "rmi:response";

    protected static final String MSG_PROP_REQUEST = "request";
    protected static final String MSG_PROP_OBJECT = "object";

    public static final JMSRemoteSystem INSTANCE = createJMSRemoteSystem();

    static class RemoteIdentity {
        final Remote remote;

        RemoteIdentity(Remote remote) {
            this.remote = remote;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteIdentity remoteIdentity = (RemoteIdentity) o;
            return remote == remoteIdentity.remote;
        }

        @Override
        public int hashCode() {
            return remote.hashCode();
        }
    }

    protected final ConcurrentHashMap<RemoteIdentity, JMSRemoteRef> exportedRemoteRefs = new ConcurrentHashMap<RemoteIdentity, JMSRemoteRef>();
    protected final ConcurrentHashMap<Long, Skeleton> exportedSkeletonsById = new ConcurrentHashMap<Long, Skeleton>();
    protected final ConcurrentHashMap<Destination, ExplictDestinationSkeleton> exportedSkeletonsByDestination = new ConcurrentHashMap<Destination, ExplictDestinationSkeleton>();

    protected final ConcurrentHashMap<Long, RequestExchange> requests = new ConcurrentHashMap<Long, RequestExchange>();
    protected final AtomicLong objectCounter = new AtomicLong(0);
    protected final AtomicLong requestCounter = new AtomicLong(0);
    protected final AtomicBoolean running = new AtomicBoolean(true);

    protected final JMSTemplate sendTemplate = new JMSTemplate(this);
    protected final JMSTemplate receiveTemplate = new JMSTemplate(this);

    protected ExecutorService senderThread;
    protected ExecutorService dispatchThreads;
    protected Thread receiveThread;
    protected String systemId;

    public void reset() throws InterruptedException {
        running.set(false);
        sendTemplate.reset();
        receiveTemplate.reset();

        if( senderThread!=null ) {
            senderThread.shutdown();
            senderThread.awaitTermination(30, TimeUnit.SECONDS);
        }
        if( receiveThread!=null ) {
            receiveThread.join(30000);
        }
        if(dispatchThreads!=null ) {
            dispatchThreads.shutdown();
            dispatchThreads.awaitTermination(30, TimeUnit.SECONDS);
        }

        synchronized(this) {
            senderThread=null;
            receiveThread=null;
            dispatchThreads=null;
            systemId=null;
        }

        for (Iterator<ExplictDestinationSkeleton> iterator = exportedSkeletonsByDestination.values().iterator(); iterator.hasNext();) {
            ExplictDestinationSkeleton entry = iterator.next();
            entry.stop();
            iterator.remove();
        }

        for (Iterator<RequestExchange> iterator = requests.values().iterator(); iterator.hasNext();) {
            RequestExchange entry = iterator.next();
            entry.cancel();
            iterator.remove();
        }

        exportedRemoteRefs.clear();
        exportedSkeletonsById.clear();
        objectCounter.set(0);
        requestCounter.set(0);
        running.set(true);
    }

    synchronized public String getSystemId() {
        if( systemId==null ) {
            systemId = createJVMID();
        }
        return systemId;
    }

    public void export(JMSRemoteRef ref, Remote obj) throws RemoteException {
        ref.initialize(obj.getClass(), receiveTemplate.getLocalSystemQueue(), objectCounter.incrementAndGet());
        exportedSkeletonsById.put(ref.getObjectId(), new Skeleton(ref, obj));
        exportedRemoteRefs.put(new RemoteIdentity(obj), ref);
        kickReceiveThread();
    }

    public void export(JMSRemoteRef ref, Remote obj, Destination destination) throws RemoteException {
        ref.initialize(obj.getClass(), destination, objectCounter.incrementAndGet());
        ExplictDestinationSkeleton skeleton = new ExplictDestinationSkeleton(this, ref, obj);
        if( exportedSkeletonsByDestination.putIfAbsent(ref.getDestination(), skeleton) != null ) {
            throw new ExportException("Another object has arlready been exported to that destination.");
        }
        exportedSkeletonsById.put(ref.getObjectId(), skeleton);
        exportedRemoteRefs.put(new RemoteIdentity(obj), ref);
        skeleton.start();
    }

    public JMSRemoteRef getExportedRemoteRef(Remote obj) throws NoSuchObjectException {
        if( JMSRemoteRef.isRemoteProxy(obj) ) {
            return JMSRemoteRef.getJMSRemoteRefFromProxy(obj);
        }
        JMSRemoteRef ref = exportedRemoteRefs.get(new RemoteIdentity(obj));
        if (ref == null)
            throw new NoSuchObjectException("Object not exported: "+obj);
        return ref;
    }

    public boolean unexport(Remote obj, boolean force) throws InterruptedException, NoSuchObjectException {
        JMSRemoteRef ref = getExportedRemoteRef(obj);
        Skeleton skeleton = exportedSkeletonsById.remove(ref.getObjectId());
        exportedRemoteRefs.remove(new RemoteIdentity(skeleton.target));
        if( ref.getDestination()!=null ) {
            ExplictDestinationSkeleton edeo = exportedSkeletonsByDestination.remove(ref.getDestination());
            if( edeo!=null ) {
                edeo.stop();
            }
        }
        // TODO: we should wait for all inovations on the object to quiese
        return true;
    }

    public Object invoke(JMSRemoteRef JMSRemoteRef, Method method, Object[] params) throws Exception {
        // Kicks off the receiver thread...
        kickReceiveThread();
        RequestExchange requestExchange = new RequestExchange(this, JMSRemoteRef, signature(method), params);
        getSenderThread().execute(requestExchange);
        try {
            return requestExchange.getResult(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw e;
        } catch (Throwable e) {
            throw new RemoteException("Unexepected error",e);
        }
    }


    private void receiveAndDispatch() throws Exception {
        try {
            Session session = receiveTemplate.getSession();
            MessageConsumer consumer = receiveTemplate.getMessageConsumer();
            Message msg = consumer.receive(500);
            if( msg!=null ) {
                if( MSG_TYPE_REQUEST.equals(msg.getJMSType()) ) {
                    // Handle decoding the message in the dispatch thread.
                    getDispatchThreads().execute(new DispatchTask(this, (ObjectMessage)msg));
                }
                if( MSG_TYPE_RESPONSE.equals(msg.getJMSType()) ) {
                    try {
                        long request = msg.getLongProperty(MSG_PROP_REQUEST);
                        RequestExchange target = requests.remove(request);
                        if( target!=null ) {
                            Response response = null;
                            try {
                                Thread.currentThread().setContextClassLoader(target.getClass().getClassLoader());
                                response = (Response)((ObjectMessage)msg).getObject();
                                response.fromRemote=true;
                            } catch (JMSException e) {
                                target.setResponse(new Response(request, null, new UnmarshalException("Could not unmarshall response: "+e.getMessage(), e)));
                            }
                            target.setResponse(response);
                        }
                    } catch (JMSException e) {
                        // response message must not have been property formatted.
                    }
                }
            }
        } catch ( Exception e ) {
            receiveTemplate.reset();
            throw e;
        }
    }

    void sendResponse(final Request request, final Response response) {
        getSenderThread().execute(new Runnable(){
            public void run() {
                ObjectMessage msg=null;
                while( running.get() ) {
                    try {
                        Session session = sendTemplate.getSession();
                        MessageProducer producer = sendTemplate.getMessageProducer();
                        if( msg==null ) {
                            try {
                                msg = session.createObjectMessage(response);
                            } catch (JMSException e) {
                                msg = session.createObjectMessage(new Response(response.requestId, null, new MarshalException("Could not marshall response: "+e.getMessage(), e)));
                            }
                        }
                        msg.setLongProperty(MSG_PROP_REQUEST, request.requestId);
                        msg.setJMSType(MSG_TYPE_RESPONSE);
                        msg.setJMSReplyTo(sendTemplate.getLocalSystemQueue());
                        producer.send(msg.getJMSReplyTo(), msg, DeliveryMode.NON_PERSISTENT, 4, 0);
                        return;
                    } catch ( Exception e ) {
                        sendTemplate.reset();
                        // TODO: should we sleep??
                        // lets loop to retry the send..
                    }
                }
            }
        });
    }


    ///////////////////////////////////////////////////////////////////
    // Exetend to implement for a different JMS provider
    ///////////////////////////////////////////////////////////////////
    abstract protected ConnectionFactory createConnectionFactory();

    abstract protected Destination createQueue(String systemId);

    ///////////////////////////////////////////////////////////////////
    // Helper Methods...
    ///////////////////////////////////////////////////////////////////

    synchronized ExecutorService getDispatchThreads() {
        if (dispatchThreads == null) {
            dispatchThreads = Executors.newCachedThreadPool(threadFactory("RMI via JMS: service"));

        }
        return dispatchThreads;
    }

    synchronized ExecutorService getSenderThread() {
        if (senderThread==null) {
            senderThread = Executors.newSingleThreadExecutor(threadFactory("RMI via JMS: sender"));
        }
        return senderThread;
    }


    synchronized Thread kickReceiveThread() {
        if( receiveThread == null ) {
            receiveThread = new Thread() {
                @Override
                public void run() {
                    while( running.get() ) {
                        try {
                            receiveAndDispatch();
                        } catch (Exception e) {
                        }
                    }
                }
            };
            receiveThread.setName("RMI via JMS: receiver");
            receiveThread.setDaemon(true);
            receiveThread.start();
        }
        return receiveThread;
    }

    private ThreadFactory threadFactory(final String name) {
        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(name);
                thread.setDaemon(true);
                return thread;
            }
        };
    }

    private static String createJVMID() {
        String name = System.getProperty("java.rmi.server.hostname");
        if( name == null ) {
            try {
                name = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                name = "unknown";
            }
        }
        return name+":"+UUID.randomUUID();
    }

    static String signature(Method method) {
        StringBuilder sb = new StringBuilder();
        if( method.getReturnType() !=null )
            sb.append(method.getReturnType().getName());
        else
            sb.append("void");
        sb.append(' ');
        sb.append(method.getName());
        for (Class<?> type : method.getParameterTypes()) {
            sb.append(' ');
            sb.append(type.getName());
        }
        return sb.toString();
    }

    private static JMSRemoteSystem createJMSRemoteSystem() {
        try {
            return (JMSRemoteSystem) JMSRemoteSystem.class.getClassLoader().loadClass(REMOTE_SYSTEM_CLASS).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Invalid setting for the org.fusesource.rmiviajms.JMSRemoteSystem system property: "+e, e);
        }
    }

}