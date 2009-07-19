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

import javax.jms.Destination;
import java.rmi.server.*;
import java.rmi.RemoteException;
import java.rmi.Remote;
import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author chirino
 */
final public class JMSRemoteRef implements RemoteRef {
    
    private Destination destination;
    private long objectId;
    private Class<?>[] interfaces;

    transient private Remote proxy;

    public JMSRemoteRef() {
    }

    public void initialize(Class<? extends Remote> clazz, Destination destination, long objectId) throws RemoteException {
        this.destination = destination;
        this.objectId = objectId;
        initialize(clazz);
    }

    private void initialize(Class<? extends Remote> clazz) throws RemoteException {
        LinkedHashSet<Class<?>> rc = new LinkedHashSet<Class<?>>();
        collectRemoteInterfaces(clazz, rc);
        if( rc.isEmpty() ) {
            throw new ExportException("No remote interfaces found.");
        }
        this.interfaces = new Class<?>[rc.size()];
        rc.toArray(interfaces);
        this.proxy = (Remote) Proxy.newProxyInstance(clazz.getClassLoader(), interfaces, new RemoteObjectInvocationHandler(this));
    }


    public Remote getProxy() {
        return proxy;
    }

    public static boolean isRemoteProxy(Remote obj) {
        return Proxy.isProxyClass(obj.getClass()) &&
                        Proxy.getInvocationHandler(obj) instanceof
                                RemoteObjectInvocationHandler;
    }

    public static JMSRemoteRef getJMSRemoteRefFromProxy(Remote obj) {
        RemoteObjectInvocationHandler handler = (RemoteObjectInvocationHandler) Proxy.getInvocationHandler(obj);
        return (JMSRemoteRef)handler.getRef();
    }

    static private void collectRemoteInterfaces(Class clazz, Set<Class<?>> rc) throws RemoteException {
        for (Class interf : clazz.getInterfaces()) {
            if( Remote.class.isAssignableFrom(interf)) {
                validateRemoteInterface(interf);
                rc.add(interf);
            }
        }
        // Also add interfaces in the super classes...
        if( clazz.getSuperclass()!=null ) {
            collectRemoteInterfaces(clazz.getSuperclass(), rc);
        }
    }

    private static void validateRemoteInterface(Class intf) throws RemoteException {
        for (Method method : intf.getMethods()) {
            validateRemoteMethod(method);
        }
    }

    private static void validateRemoteMethod(Method method) throws RemoteException {
        boolean throwsRemoteException=false;
        for (Class<?> etx : method.getExceptionTypes()) {
            if( RemoteException.class.isAssignableFrom(etx) ) {
                throwsRemoteException=true;
                break;
            }
        }
        if( !throwsRemoteException ) {
            throw new ExportException("Invlaid Remote interface "+method.getDeclaringClass().getName()+" method "+method.getName()+" does not throw a RemoteException");
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
        if( obj.getClass() != JMSRemoteRef.class )
            return false;
        JMSRemoteRef other = (JMSRemoteRef) obj;
        return other.objectId == objectId && other.destination.equals(destination);
    }

    public String remoteToString() {
        return destination +":"+objectId;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(destination);
        out.writeLong(objectId);
        out.writeShort(interfaces.length);
        for (Class<?> i : interfaces) {
            out.writeUTF(i.getName());
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        destination = (Destination) in.readObject();
        objectId = in.readLong();
        interfaces = new Class<?>[in.readShort()];
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (int i=0; i < interfaces.length; i++) {
            interfaces[i] = cl.loadClass(in.readUTF());
        }
        this.proxy = (Remote) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, new RemoteObjectInvocationHandler(this));
    }

//    protected Object readResolve() throws ObjectStreamException {
//        return proxy;
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