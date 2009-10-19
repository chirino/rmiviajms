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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.jms.Destination;

import org.fusesource.rmiviajms.Oneway;

/**
 * 
 */
@SuppressWarnings("deprecation")
final public class JMSRemoteRef implements RemoteRef {

    //List of Annotations signifying Oneway (async) operations.
    //Third party apps using this may supply their own annotations
    //that indicate this
    private static final HashSet<Class<? extends Annotation>> ONE_WAY_ANNOTATIONS = new HashSet<Class<? extends Annotation>>();
    static {
        ONE_WAY_ANNOTATIONS.add(Oneway.class);
    }

    private Destination destination;
    private long objectId;

    //If this is a CGLib Reference then this will point to the 
    //superclass being proxied:
    private Class<?> superclass;
    //Service interface exposed by the proxy (will always contain
    //Remote:
    private Class<?>[] interfaces;

    //If the exported object conforms strictly to RMI e.g.
    //it implements Remote and throws RemoteException. 
    //For CGLib proxies and lax remoting, this will be false.
    private boolean isRemote;

    //The proxy class (Either a java.lang.reflect.Proxy or
    //CGLib enhanced subclass:
    transient private Remote proxy;

    public JMSRemoteRef() {
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

        if (interfaces != null && interfaces.length > 0) {
            for (Class<?> c : interfaces) {
                if (!c.isAssignableFrom(clazz)) {
                    throw new RemoteException("Invalid proxy interface " + clazz.getName() + " not assignable to " + c.getName());
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
            this.proxy = (Remote) Proxy.newProxyInstance(clazz.getClassLoader(), this.interfaces, createInvocationHandler());
        } else {
            for (Method m : clazz.getDeclaredMethods()) {
                validateRemoteMethod(m, false);
            }
            this.superclass = clazz;
            this.interfaces = new Class[] { Remote.class };
            this.proxy = (Remote) CGLibProxyAdapter.newProxyInstance(clazz, this.interfaces, createInvocationHandler());
        }
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
        this.proxy = (Remote) Proxy.newProxyInstance(clazz.getClassLoader(), interfaces, createInvocationHandler());
    }

    private InvocationHandler createInvocationHandler() {
        if( isRemote ) {
            return new RemoteObjectInvocationHandler(this);
        } else {
            return new JMSRemoteObjectInvocationHandler(this);
        }
    }

    private void initialize(List<Class<?>> interfaces, Destination destination, boolean validateRemote) throws RemoteException {
        this.destination = destination;
        this.objectId = -1;
        this.interfaces = new Class<?>[interfaces.size()];
        interfaces.toArray(this.interfaces);
        this.proxy = (Remote) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), this.interfaces, createInvocationHandler());
    }

    public Remote getProxy() {
        return proxy;
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

    /**
     * Tests if a given method has a one way annotation
     * 
     * @param method
     *            The mehod.
     */
    static boolean isOneWay(Method method) {
        for (Class<? extends Annotation> annotation : ONE_WAY_ANNOTATIONS) {
            if (method.isAnnotationPresent(annotation)) {
                return true;
            }
        }
        return false;
    }

    public static <T> T toProxy(String destination, Class<T> mainClass, Class<?>... extraInterface) throws RemoteException {
        if (mainClass == null) {
            throw new IllegalArgumentException("mainClass cannot be null.");
        }

        JMSRemoteRef ref = new JMSRemoteRef();
        ArrayList<Class<?>> list = new ArrayList<Class<?>>(extraInterface.length+1);
        list.add(mainClass);
        if (extraInterface != null) {
            list.addAll(Arrays.asList(extraInterface));
        }

        for (Class<?> c : list) {
            if (!c.isInterface()) {
                throw new IllegalArgumentException("Not an interface: " + c);
            }
        }

        // We may need to add the Remote interface..
        if( !list.contains(Remote.class)) {
            list.add(Remote.class);
        }

        ref.initialize(list, JMSRemoteSystem.createDestination(destination), false);
        return (T) ref.getProxy();
    }

    public static boolean isRemoteProxy(Object obj) {
        return getProxyInvocationHandler(obj) != null;
    }

    private static RemoteObjectInvocationHandler getProxyInvocationHandler(Object obj) {
        InvocationHandler handler = null;
        if (Proxy.isProxyClass(obj.getClass())) {
            handler = Proxy.getInvocationHandler(obj);
        } else if (CGLibProxyAdapter.isProxyClass(obj.getClass())) {
            handler = CGLibProxyAdapter.getInvocationHandler(obj);
        }

        if (handler instanceof RemoteObjectInvocationHandler) {
            return (RemoteObjectInvocationHandler) handler;
        }
        return null;
    }

    public static JMSRemoteRef getJMSRemoteRefFromProxy(Remote obj) {
        RemoteObjectInvocationHandler handler = getProxyInvocationHandler(obj);
        if (handler != null) {
            return (JMSRemoteRef) handler.getRef();
        }
        return null;
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
        if (isOneWay(method) && method.getReturnType() != void.class) {
            throw new ExportException("Invalid remote class " + method.getDeclaringClass().getName() + " method " + method.getName() + " is annotated as OneWay so it must return void");
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

        boolean isCGProxy = CGLibProxyAdapter.isProxyClass(proxy.getClass());
        out.writeBoolean(isCGProxy);
        if (isCGProxy) {
            out.writeUTF(superclass.getName());
        }

        out.writeShort(interfaces.length);
        for (Class<?> i : interfaces) {
            out.writeUTF(i.getName());
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        destination = (Destination)in.readObject();
        isRemote = in.readBoolean();
        objectId = in.readLong();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        boolean isCGProxy = in.readBoolean();
        if (isCGProxy) {
            superclass = cl.loadClass(in.readUTF());
        }

        interfaces = new Class<?>[in.readShort()];
        for (int i = 0; i < interfaces.length; i++) {
            interfaces[i] = cl.loadClass(in.readUTF());
        }

        if (!isCGProxy) {
            this.proxy = (Remote) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, createInvocationHandler());
        } else {
            this.proxy = (Remote) CGLibProxyAdapter.newProxyInstance(superclass, interfaces, createInvocationHandler());
        }
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