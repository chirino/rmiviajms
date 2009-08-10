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
package org.fusesource.rmiviajms;

import org.fusesource.rmiviajms.internal.JMSRemoteRef;
import org.fusesource.rmiviajms.internal.JMSRemoteSystem;

import javax.jms.Destination;
import java.rmi.server.*;
import java.rmi.RemoteException;
import java.rmi.Remote;
import java.rmi.NoSuchObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.io.InvalidObjectException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author chirino
 */
public class JMSRemoteObject extends RemoteObject implements Serializable {

    protected JMSRemoteObject() throws RemoteException {
        exportObject(this);
    }

    protected JMSRemoteObject(Destination destination) throws RemoteException {
        exportObject(this, destination);
    }

    public Object clone() throws CloneNotSupportedException {
        try {
            JMSRemoteObject cloned = (JMSRemoteObject) super.clone();
            exportObject(cloned);
            return cloned;
        } catch (RemoteException e) {
            throw new ServerCloneException("Clone failed", e);
        }
    }

    public static void addOneWayAnnotation(Class<? extends Annotation> annotation) {
        JMSRemoteRef.addOneWayAnnotation(annotation);
    }

    public static Remote exportNonRemote(Object obj, Class<?>[] interfaces) throws Exception {
        return exportNonRemote(obj, interfaces, null);
    }

    public static Remote exportNonRemote(Object obj, Class<?>[] interfaces, Destination destination) throws Exception {
        JMSRemoteRef ref = new JMSRemoteRef();
        if (destination == null) {
            JMSRemoteSystem.INSTANCE.exportNonRemote(obj, interfaces, ref);
        } else {
            JMSRemoteSystem.INSTANCE.exportNonRemote(obj, interfaces, destination, ref);
        }
        return ref.getProxy();
    }

    public static Remote exportObject(Remote obj) throws RemoteException {
        return exportObject(obj, null);
    }

    public static Remote exportObject(Remote obj, Destination destination) throws RemoteException {
        JMSRemoteRef ref = new JMSRemoteRef();
        if (obj instanceof JMSRemoteObject) {
            ((JMSRemoteObject) obj).ref = ref;
        }
        if (destination == null) {
            JMSRemoteSystem.INSTANCE.export(ref, obj);
        } else {
            JMSRemoteSystem.INSTANCE.export(ref, obj, destination);
        }
        return ref.getProxy();
    }

    public static boolean unexportObject(Remote obj, boolean force) throws java.rmi.NoSuchObjectException {
        try {
            return JMSRemoteSystem.INSTANCE.unexport(obj, force);
        } catch (NoSuchObjectException e) {
            // Maybe it's  a traditional UnicastRemoteObject
            try {
                return UnicastRemoteObject.unexportObject(obj, force);
            } catch (NoSuchObjectException e2) {
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static void resetSystem() throws InterruptedException {
        JMSRemoteSystem.INSTANCE.reset();
    }

    public static Remote toStub(Remote obj) throws NoSuchObjectException {
        if (JMSRemoteRef.isRemoteProxy(obj))
            return obj;
        JMSRemoteRef ref = JMSRemoteSystem.INSTANCE.getExportedRemoteRef(obj);
        return ref.getProxy();
    }

    protected Object writeReplace() throws ObjectStreamException {
        return ((JMSRemoteRef) ref).getProxy();
    }

    public static <T extends Remote> T toProxy(Destination destination, Class<T> mainInterface, Class<? extends Remote>... additionalInterface) throws RemoteException {
        if (mainInterface == null) {
            throw new IllegalArgumentException("mainInterface cannot be null.");
        }

        ArrayList<Class<?>> list = new ArrayList<Class<?>>();
        list.add(mainInterface);
        if (additionalInterface != null) {
            for (Class<? extends Remote> r : additionalInterface) {
                list.add(r);
            }
        }

        return (T) JMSRemoteRef.toProxy(destination, list);
    }

    static final private ThreadLocal<Long> NEXT_INVOCATION_TIMEOUT = new ThreadLocal<Long>();

    public static void setNextInvocationTimeout(Long timeout) {
        NEXT_INVOCATION_TIMEOUT.set(timeout);
    }

    public static Long removeNextInvocationTimeout() {
        try {
            return NEXT_INVOCATION_TIMEOUT.get();
        } finally {
            NEXT_INVOCATION_TIMEOUT.set(null);
        }
    }

}