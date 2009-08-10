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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.rmi.server.Operation;
import java.rmi.server.RemoteCall;
import java.rmi.server.RemoteObject;
import java.rmi.server.RemoteObjectInvocationHandler;
import java.rmi.server.RemoteRef;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.jms.Destination;

import org.fusesource.rmiviajms.Oneway;

/**
 * @author chirino
 */
final public class JMSRemoteRef implements RemoteRef {

    private static final HashSet<Class<? extends Annotation>> ONE_WAY_ANNOTATIONS = new HashSet<Class<? extends Annotation>>();
    static {
        ONE_WAY_ANNOTATIONS.add(Oneway.class);
    }

    private Destination destination;
    private long objectId;
    private Class<?>[] interfaces;
    private boolean isRemote;

    transient private Remote proxy;

    public JMSRemoteRef() {
    }

    /**
     * Adds an annotation to the set of annotations that signify that a method
     * is a one way method.
     * 
     * @param annotation
     *            The annotation.
     */
    public static void addOneWayAnnotation(Class<? extends Annotation> annotation) {
        ONE_WAY_ANNOTATIONS.add(annotation);
    }

    static boolean isOneWay(Method method) {
       for (Class<? extends Annotation> annotation : ONE_WAY_ANNOTATIONS) {
            if (method.isAnnotationPresent(annotation)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Provices support for objects that don't implement Remote, or wish to
     * export a set of interfaces different from those extending Remote.
     * 
     * @param clazz
     *            The class to export.
     * @param interfaces
     *            The interfaces to expose
     * @param destination
     *            The destination to export to (possibly null)
     * @param objectId
     * @throws Exception
     */
    public void initializeNonRemote(Class<?> clazz, Class<?>[] interfaces, Destination destination, long objectId) throws Exception {
        this.destination = destination;
        this.objectId = objectId;
        this.isRemote = false;
        for (Class<?> c : interfaces) {
            if (!c.isAssignableFrom(clazz)) {
                throw new RemoteException("Invalid Remote interface " + clazz.getName() + " not assignable to " + c.getName());
            }

            //Make sure we're passed an interface
            if (!c.isInterface()) {
                throw new IllegalArgumentException("Not an interface: " + c);
            }

            validateRemoteInterface(c, false);
        }

        this.interfaces = new Class<?>[interfaces.length + 1];
        System.arraycopy(interfaces, 0, this.interfaces, 0, interfaces.length);
        this.interfaces[this.interfaces.length - 1] = Remote.class;
        this.proxy = (Remote) Proxy.newProxyInstance(clazz.getClassLoader(), this.interfaces, new JMSRemoteObjectInvocationHandler(this));
    }

    public void initialize(Class<? extends Remote> clazz, Destination destination, long objectId) throws RemoteException {
        this.destination = destination;
        this.objectId = objectId;
        this.isRemote = true;
        initialize(clazz);
    }

    private void initialize(Class<? extends Remote> clazz) throws RemoteException {
        LinkedHashSet<Class<?>> rc = new LinkedHashSet<Class<?>>();
        collectRemoteInterfaces(clazz, rc);
        if (rc.isEmpty()) {
            throw new ExportException("No remote interfaces found.");
        }
        this.interfaces = new Class<?>[rc.size()];
        rc.toArray(interfaces);
        this.proxy = (Remote) Proxy.newProxyInstance(clazz.getClassLoader(), interfaces, new JMSRemoteObjectInvocationHandler(this));
    }

    private void initialize(List<Class<?>> interfaces, Destination destination) throws RemoteException {
        for (Class<?> interf : interfaces) {
            if (!Remote.class.isAssignableFrom(interf)) {
                throw new RemoteException("Invalid Remote interface " + interf.getName());
            }
        }
        this.destination = destination;
        this.objectId = -1;
        this.interfaces = new Class<?>[interfaces.size()];
        interfaces.toArray(this.interfaces);
        this.proxy = (Remote) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), this.interfaces, new JMSRemoteObjectInvocationHandler(this));
    }

    public static <T extends Remote> T toProxy(Destination destination, List<Class<?>> list) throws RemoteException {
        JMSRemoteRef ref = new JMSRemoteRef();
        ref.initialize(list, destination);
        return (T) ref.getProxy();
    }

    public Remote getProxy() {
        return proxy;
    }

    public static boolean isRemoteProxy(Remote obj) {
        return Proxy.isProxyClass(obj.getClass()) && Proxy.getInvocationHandler(obj) instanceof RemoteObjectInvocationHandler;
    }

    public static JMSRemoteRef getJMSRemoteRefFromProxy(Remote obj) {
        RemoteObjectInvocationHandler handler = (RemoteObjectInvocationHandler) Proxy.getInvocationHandler(obj);
        return (JMSRemoteRef) handler.getRef();
    }

    static private void collectRemoteInterfaces(Class<?> clazz, Set<Class<?>> rc) throws RemoteException {
        for (Class<?> interf : clazz.getInterfaces()) {
            if (Remote.class.isAssignableFrom(interf)) {
                validateRemoteInterface(interf, true);
                rc.add(interf);
            }
        }
        // Also slowOnewayOperations interfaces in the super classes...
        if (clazz.getSuperclass() != null) {
            collectRemoteInterfaces(clazz.getSuperclass(), rc);
        }
    }

    private static void validateRemoteInterface(Class<?> intf, boolean isRemote) throws RemoteException {
        for (Method method : intf.getMethods()) {
            validateRemoteMethod(method, isRemote);
        }
    }

    private static void validateRemoteMethod(Method method, boolean isRemote) throws RemoteException {
        if (isRemote) {
            boolean throwsRemoteException = false;
            for (Class<?> etx : method.getExceptionTypes()) {
                if (RemoteException.class.isAssignableFrom(etx) || etx.isAssignableFrom(RemoteException.class)) {
                    throwsRemoteException = true;
                    break;
                }
            }
            if (!throwsRemoteException) {
                throw new ExportException("Invalid Remote interface " + method.getDeclaringClass().getName() + " method " + method.getName() + " does not throw a RemoteException");
            }
        }
        if (method.isAnnotationPresent(Oneway.class) && method.getReturnType() != void.class) {
            throw new ExportException("Invalid Remote interface " + method.getDeclaringClass().getName() + " method " + method.getName() + " is annotated with @Oneway so it must return void");
        }
    }

    // RemoteRef interface

    public Object invoke(Remote obj, Method method, Object[] params, long opnum) throws Exception {
        return JMSRemoteSystem.INSTANCE.invoke(this, method, params);
    }

    public String getRefClass(ObjectOutput out) {
        return null;
    }

    public int remoteHashCode() {
        return destination.hashCode() ^ new Long(objectId).hashCode();
    }

    public boolean remoteEquals(RemoteRef obj) {
        if (obj.getClass() != JMSRemoteRef.class)
            return false;
        JMSRemoteRef other = (JMSRemoteRef) obj;
        return other.objectId == objectId && other.destination.equals(destination);
    }

    public String remoteToString() {
        return destination + ":" + objectId;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(destination);
        out.writeBoolean(isRemote);
        out.writeLong(objectId);
        out.writeShort(interfaces.length);
        for (Class<?> i : interfaces) {
            out.writeUTF(i.getName());
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        destination = (Destination) in.readObject();
        isRemote = in.readBoolean();
        objectId = in.readLong();
        interfaces = new Class<?>[in.readShort()];
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < interfaces.length; i++) {
            interfaces[i] = cl.loadClass(in.readUTF());
        }
        this.proxy = (Remote) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, new JMSRemoteObjectInvocationHandler(this));
    }

    //    protected Object readResolve() throws ObjectStreamException {
    //        return toProxy;
    //    }

    @Deprecated
    public RemoteCall newCall(RemoteObject obj, Operation[] op, int opnum, long hash) throws RemoteException {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public void invoke(RemoteCall call) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public void done(RemoteCall call) throws RemoteException {
        throw new UnsupportedOperationException();
    }

    // Accessors
    public Class<?>[] getInterfaces() {
        return interfaces;
    }

    public long getObjectId() {
        return objectId;
    }

    public Destination getDestination() {
        return destination;
    }

}