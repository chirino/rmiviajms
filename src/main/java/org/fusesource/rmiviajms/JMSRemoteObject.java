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

    /**
     * Adds an additional annotation that will be used to interpret whether or
     * not a method is one way (async). This can be useful for cases where
     * applications want to avoid having compile time dependencies on rmi via
     * jms.
     * 
     * @param annotation
     *            The annotation signifying one way methods.
     */
    public static void addOneWayAnnotation(Class<? extends Annotation> annotation) {
        JMSRemoteRef.addOneWayAnnotation(annotation);
    }

    /**
     * Same as {@link #export(Object, javax.jms.Destination, Class[])} but uses the
     * default local destination.
     * 
     * @param obj
     *            The object to export
     * @param interfaces
     *            The interfaces to export.
     * @return The proxy.
     * @throws Exception
     *             If there is an error exporting the object
     */
    public static Remote export(Object obj, Class<?>... interfaces) throws Exception {
        return export(obj, null, interfaces);
    }

    /**
     * Exports an object. Unlike {@link #exportObject(Remote, Destination)}, the
     * provided object does <i>not</i> have to implement the {@link Remote}
     * interface. If there is an error during remote method invocation on the
     * returned stub a RuntimeException will be thrown. This is useful in cases
     * where application do not want to back in dependenies on java.rmi.
     * 
     * @param obj
     *            The object to export
     * @param interfaces
     *            The interfaces to export.
     * @param destination
     *            The destination at which to expose the object.
     * @return The proxy.
     * @throws Exception
     *             If there is an error exporting the object
     */
    public static Remote export(Object obj, Destination destination, Class<?>... interfaces) throws Exception {
        //If this is already a remote object just return:
        if (JMSRemoteRef.isRemoteProxy(obj)) {
            return (Remote) obj;
        }

        JMSRemoteRef ref = new JMSRemoteRef();
        if (destination == null) {
            JMSRemoteSystem.INSTANCE.exportNonRemote(obj, interfaces, ref);
        } else {
            JMSRemoteSystem.INSTANCE.exportNonRemote(obj, interfaces, destination, ref);
        }
        return ref.getProxy();
    }

    /**
     * Same as {@link #export(Object, Class[])}, where the interfaces are all of
     * those in the provided object that extend {@link Remote}.
     * 
     * @param obj
     *            The object to export
     * @return The proxy.
     * @throws RemoteException
     *             If there is an error distributing the object.
     */
    public static Remote exportObject(Remote obj) throws RemoteException {
        return exportObject(obj, null);
    }

    /**
     * Same as {@link #export(Object, javax.jms.Destination, Class[])} , where the
     * interfaces are all of those in the provided object that extend
     * {@link Remote}.
     * 
     * @param obj
     *            The object to export
     * @param destination
     *            The destination at which to expose the object.
     * @return The proxy.
     * @throws RemoteException
     *             If there is an error distributing the object.
     */
    public static Remote exportObject(Remote obj, Destination destination) throws RemoteException {
        //If this is already a remote object just return:
        if (JMSRemoteRef.isRemoteProxy(obj)) {
            return (Remote) obj;
        }

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

    /**
     * Unexports a given object. This can be called with eitehr the returned
     * proxy stub or the originally exported object.
     * 
     * @param obj
     *            The proxy or exported object
     * @param force
     *            if true, unexports the object even if there are pending or
     *            in-progress calls; if false, only unexports the object if
     *            there are no pending or in-progress calls
     * 
     * @return true if unexport was successful.
     * @throws java.rmi.NoSuchObjectException
     *             If the object couldn't be found.
     */
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

    public static <T> T toProxy(Destination destination, Class<T> mainInterface, Class<?>... extraInterface) throws RemoteException {
        return (T) JMSRemoteRef.toProxy(destination, mainInterface, extraInterface);
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