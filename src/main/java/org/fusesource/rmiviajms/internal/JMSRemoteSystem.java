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

import org.fusesource.rmiviajms.*;
import org.fusesource.rmiviajms.internal.JMSTemplate.TemplateClosedException;

import javax.jms.*;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.*;
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
    public static final long REQUEST_TIMEOUT = new Long(System.getProperty("org.fusesource.rmiviajms.REQUEST_TIMEOUT", "" + Long.MAX_VALUE));

    protected static final String MSG_TYPE_ONEWAY = "rmi:oneway";
    protected static final String MSG_TYPE_REQUEST = "rmi:request";
    protected static final String MSG_TYPE_RESPONSE = "rmi:response";

    protected static final String MSG_PROP_REQUEST = "request";
    protected static final String MSG_PROP_OBJECT = "object";

    public static final JMSRemoteSystem INSTANCE = createJMSRemoteSystem();

    public ClassLoader userClassLoader;

    static class RemoteIdentity {
        final Object remote;

        RemoteIdentity(Object remote) {
            this.remote = remote;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
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

    protected final ConcurrentHashMap<Long, RequestExchange> requests = new ConcurrentHashMap<Long, RequestExchange>();
    protected final AtomicLong objectCounter = new AtomicLong(0);
    protected final AtomicLong requestCounter = new AtomicLong(0);
    protected final AtomicBoolean running = new AtomicBoolean(true);

    protected JMSTemplate sendTemplate = new JMSTemplate(this);
    protected JMSTemplate receiveTemplate = new JMSTemplate(this);

    protected ExecutorService senderThread;
    protected ExecutorService dispatchThreads;
    protected Thread receiveThread;
    protected String systemId;

    public void setUserClassLoader(ClassLoader userClassLoader) {
        this.userClassLoader = userClassLoader;
    }

    public ClassLoader getUserClassLoader() {
        return userClassLoader;
    }

    /**
     * Gets the user class loader if set, returning the class loader of the
     * given object other wise.
     * 
     * @param o
     *            The object whose class loader to default to.
     * @return A class loader appropriate for the given object
     */
    public ClassLoader getUserClassLoader(Object o) {
        if (userClassLoader != null) {
            return userClassLoader;
        } else if (o != null) {
            return o.getClass().getClassLoader();
        } else {
            return Thread.currentThread().getContextClassLoader();
        }
    }

    public void reset() throws InterruptedException {
        running.set(false);
        synchronized (this) {
            sendTemplate.close();
            receiveTemplate.close();

            if (senderThread != null) {
                senderThread.shutdown();
                senderThread.awaitTermination(30, TimeUnit.SECONDS);
            }
            if (receiveThread != null) {
                receiveThread.join(30000);
            }
            if (dispatchThreads != null) {
                dispatchThreads.shutdown();
                dispatchThreads.awaitTermination(30, TimeUnit.SECONDS);
            }

            senderThread = null;
            receiveThread = null;
            dispatchThreads = null;
            systemId = null;

            for (Iterator<Skeleton> iterator = exportedSkeletonsById.values().iterator(); iterator.hasNext();) {
                Skeleton entry = iterator.next();
                if (entry instanceof ExplictDestinationSkeleton) {
                    ((ExplictDestinationSkeleton) entry).stop();
                }
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

            sendTemplate = new JMSTemplate(this);
            receiveTemplate = new JMSTemplate(this);
        }
        running.set(true);
    }

    synchronized public String getSystemId() {
        if (systemId == null) {
            systemId = createJVMID();
        }
        return systemId;
    }

    /**
     * @param ref
     * @param obj
     */
    public void exportNonRemote(Object obj, Class<?>[] interfaces, JMSRemoteRef ref) throws Exception {
        ref.initializeNonRemote(obj.getClass(), interfaces, receiveTemplate.getLocalSystemQueue(), objectCounter.incrementAndGet());
        exportedSkeletonsById.put(ref.getObjectId(), new Skeleton(this, ref, obj));
        exportedRemoteRefs.put(new RemoteIdentity(obj), ref);
        kickReceiveThread();
    }

    /**
     * @param ref
     * @param obj
     * @param destination
     */
    public void exportNonRemote(Object obj, Class<?>[] interfaces, String destination, JMSRemoteRef ref) throws Exception {
        ref.initializeNonRemote(obj.getClass(), interfaces, createDestination(destination), objectCounter.incrementAndGet());
        ExplictDestinationSkeleton skeleton = new ExplictDestinationSkeleton(this, ref, obj);
        exportedSkeletonsById.put(ref.getObjectId(), skeleton);
        exportedRemoteRefs.put(new RemoteIdentity(obj), ref);
        try {
            skeleton.start();
        } catch (Exception e) {
            throw new RemoteException("Error exporting object", e);
        }
    }

    public void export(JMSRemoteRef ref, Remote obj) throws RemoteException {
        ref.initialize(obj.getClass(), receiveTemplate.getLocalSystemQueue(), objectCounter.incrementAndGet());
        exportedSkeletonsById.put(ref.getObjectId(), new Skeleton(this, ref, obj));
        exportedRemoteRefs.put(new RemoteIdentity(obj), ref);
        try {
            kickReceiveThread();
        } catch (TemplateClosedException tce) {
            throw new RemoteException("RemoteSystem reset", tce);
        }
    }

    public void export(JMSRemoteRef ref, Remote obj, String destination) throws RemoteException {
        ref.initialize(obj.getClass(), createDestination(destination), objectCounter.incrementAndGet());
        ExplictDestinationSkeleton skeleton = new ExplictDestinationSkeleton(this, ref, obj);
        exportedSkeletonsById.put(ref.getObjectId(), skeleton);
        exportedRemoteRefs.put(new RemoteIdentity(obj), ref);
        try {
            skeleton.start();
        } catch (Exception e) {
            throw new RemoteException("Error exporting object", e);
        }
    }

    public JMSRemoteRef getExportedRemoteRef(Remote obj) throws NoSuchObjectException {
        if (JMSRemoteRef.isRemoteProxy(obj)) {
            return JMSRemoteRef.getJMSRemoteRefFromProxy(obj);
        }
        JMSRemoteRef ref = exportedRemoteRefs.get(new RemoteIdentity(obj));
        if (ref == null) {
            throw new NoSuchObjectException("Object not exported: " + obj);
        }
        return ref;
    }

    public boolean unexport(Remote obj, boolean force) throws InterruptedException, NoSuchObjectException {
        JMSRemoteRef ref = getExportedRemoteRef(obj);
        Skeleton skeleton = exportedSkeletonsById.remove(ref.getObjectId());
        if (skeleton == null) {
            throw new NoSuchObjectException("Object not exported: " + obj);
        }
        exportedRemoteRefs.remove(new RemoteIdentity(skeleton.target));
        if (skeleton instanceof ExplictDestinationSkeleton) {
            ((ExplictDestinationSkeleton) skeleton).stop();
        }
        // TODO: we should wait for all inovations on the object to quiese
        return true;
    }

    public Object invoke(JMSRemoteRef jmsRemoteRef, Method method, Object[] params) throws Exception {

        boolean oneway = JMSRemoteRef.isOneWay(method);

        long timeout = 0;
        if (!oneway) {
            timeout = REQUEST_TIMEOUT;
            if (method.isAnnotationPresent(Timeout.class)) {
                timeout = method.getAnnotation(Timeout.class).value();
            }

            // Perhaps there is per inovocation timeout configured..
            Long nto = JMSRemoteObject.removeNextInvocationTimeout();
            if (nto != null) {
                timeout = nto;
            }

            // Kicks off the receiver thread...
            kickReceiveThread();
        }

        int deliveryMode = method.isAnnotationPresent(Persistent.class) ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        int priority = 4;
        if (method.isAnnotationPresent(Priority.class)) {
            priority = method.getAnnotation(Priority.class).value();
        }

        RequestExchange requestExchange = new RequestExchange(this, jmsRemoteRef, signature(method), params, oneway, timeout, deliveryMode, priority);
        getSenderThread().execute(requestExchange);
        try {
            return requestExchange.getResult();
        } catch (Exception e) {
            throw e;
        } catch (Throwable e) {
            throw new RemoteException("Unexepected error", e);
        }
    }

    private void receiveAndDispatch() throws Exception {
        try {
            Session session = receiveTemplate.getSession();
            MessageConsumer consumer = receiveTemplate.getMessageConsumer();
            Message msg = consumer.receive(500);
            if (msg != null) {
                if (MSG_TYPE_REQUEST.equals(msg.getJMSType())) {
                    // Handle decoding the message in the dispatch thread.
                    getDispatchThreads().execute(new DispatchTask(this, (ObjectMessage) msg, false));
                } else if (MSG_TYPE_ONEWAY.equals(msg.getJMSType())) {
                    // Handle decoding the message in the dispatch thread.
                    getDispatchThreads().execute(new DispatchTask(this, (ObjectMessage) msg, true));
                } else if (MSG_TYPE_RESPONSE.equals(msg.getJMSType())) {
                    try {
                        long request = msg.getLongProperty(MSG_PROP_REQUEST);
                        RequestExchange target = requests.remove(request);
                        if (target != null) {
                            Response response = null;
                            try {
                                Thread.currentThread().setContextClassLoader(getUserClassLoader(target));
                                response = (Response) ((ObjectMessage) msg).getObject();
                                response.fromRemote = true;
                            } catch (JMSException e) {
                                target.setResponse(new Response(request, null, new UnmarshalException("Could not unmarshall response: " + e.getMessage(), e)));
                            }
                            target.setResponse(response);
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (TemplateClosedException tce) {
            //TODO we should probably just eat this.
            tce.printStackTrace();
            throw tce;
        } catch (Exception e) {
            e.printStackTrace();
            receiveTemplate.reset();
            throw e;
        }
    }

    void sendResponse(final Message requestMessage, final Response response) {
        getSenderThread().execute(new Runnable() {
            public void run() {
                ObjectMessage msg = null;
                while (running.get()) {
                    try {
                        Session session = sendTemplate.getSession();
                        MessageProducer producer = sendTemplate.getMessageProducer();
                        if (msg == null) {
                            try {
                                msg = session.createObjectMessage(response);
                            } catch (JMSException e) {
                                msg = session.createObjectMessage(new Response(response.requestId, null, new MarshalException("Could not marshall response: " + e.getMessage(), e)));
                            }
                        }
                        msg.setLongProperty(MSG_PROP_REQUEST, response.requestId);
                        msg.setJMSType(MSG_TYPE_RESPONSE);
                        producer.send(requestMessage.getJMSReplyTo(), msg, requestMessage.getJMSDeliveryMode(), requestMessage.getJMSPriority(), 0);
                        return;
                    } catch (TemplateClosedException tce) {
                        //TODO we should probably just eat this.
                        tce.printStackTrace();
                        return;
                    } catch (Exception e) {
                        e.printStackTrace();
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

    abstract protected Destination createQueue(String queueId);

    abstract protected Destination createTopic(String topicId);

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
        if (senderThread == null) {
            senderThread = Executors.newSingleThreadExecutor(threadFactory("RMI via JMS: sender"));
        }
        return senderThread;
    }

    synchronized Thread kickReceiveThread() throws TemplateClosedException {
        if (receiveThread == null) {

            //Make sure our consumer is created in this thread, so as not
            //to miss invocation requests.
            while (true) {
                try {
                    receiveTemplate.getMessageConsumer();
                    break;
                } catch (JMSException e1) {
                    receiveTemplate.reset();
                }
            }
            receiveThread = new Thread() {
                @Override
                public void run() {
                    while (running.get()) {
                        try {
                            receiveAndDispatch();
                        } catch (TemplateClosedException tce) {
                            return;
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
        if (name == null) {
            try {
                name = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                name = "unknown";
            }
        }
        return name + ":" + UUID.randomUUID();
    }

    static final Destination createDestination(String destination) {
        if (destination.startsWith(JMSRemoteObject.MULTICAST_PREFIX)) {
            return INSTANCE.createTopic(destination.substring(JMSRemoteObject.MULTICAST_PREFIX.length()));
        } else {
            return INSTANCE.createQueue(destination);
        }
    }

    static String signature(Method method) {
        StringBuilder sb = new StringBuilder();
        if (method.getReturnType() != null)
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
            try {
                return (JMSRemoteSystem) JMSRemoteSystem.class.getClassLoader().loadClass(REMOTE_SYSTEM_CLASS).newInstance();
            } catch (ClassNotFoundException cnfe) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                if (cl != null) {
                    //System.out.println("Using context cl: " + cl.hashCode());
                    return (JMSRemoteSystem) cl.loadClass(REMOTE_SYSTEM_CLASS).newInstance();
                } else {
                    throw cnfe;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Invalid setting for the org.fusesource.rmiviajms.JMSRemoteSystem system property: " + e, e);
        }
    }

}