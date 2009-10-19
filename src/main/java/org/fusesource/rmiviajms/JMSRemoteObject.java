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
package org.fusesource.rmiviajms;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.RemoteObject;
import java.rmi.server.ServerCloneException;
import java.rmi.server.UnicastRemoteObject;

import org.fusesource.rmiviajms.internal.JMSRemoteRef;
import org.fusesource.rmiviajms.internal.JMSRemoteSystem;

/**
 * @author chirino
 */
public class JMSRemoteObject extends RemoteObject implements Serializable {

    /**
     * Prefixing a destination string with this prefix will assign the exported
     * object to the topic following the prefix. Calls made via a proxy for an
     * object exported to such a destination will be sent to all objects exported
     * there. 
     */
    public static final String MULTICAST_PREFIX = "multicast:";
    
    protected JMSRemoteObject() throws RemoteException {
        exportObject(this);
    }

    protected JMSRemoteObject(String destination) throws RemoteException {
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
    public static Remote export(Object obj, String destination, Class<?>... interfaces) throws Exception {
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
    public static Remote exportObject(Remote obj, String destination) throws RemoteException {
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

    public static <T> T toProxy(String destination, Class<T> mainInterface, Class<?>... extraInterface) throws RemoteException {
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