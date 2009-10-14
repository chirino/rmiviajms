/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.rmiviajms.internal;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationHandler;
import java.rmi.server.ExportException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.WeakHashMap;

import net.sf.cglib.proxy.*;

/***
 * Adapted from CGLibs JDKCompatibleProxy sample at:
 * http://cglib.sourceforge.net/xref/samples/JdkCompatibleProxy.html
 * 
 * This class is meant to be used as a drop-in replacement for
 * <code>java.lang.reflect.Proxy</code>. There are some known subtle
 * differences:
 * <ul>
 * <li>The exceptions returned by invoking <code>getExceptionTypes</code> on the
 * <code>Method</code> passed to the <code>invoke</code> method <b>are</b> the
 * exact set that can be thrown without resulting in an
 * <code>UndeclaredThrowableException</code> being thrown.
 * <li>There is no protected constructor which accepts an
 * <code>InvocationHandler</code>. Instead, use the more convenient
 * <code>newProxyInstance</code> static method.
 * <li><code>net.sf.cglib.UndeclaredThrowableException</code> is used instead of
 * <code>java.lang.reflect.UndeclaredThrowableException</code>.
 * </ul>
 * 
 * @author Chris Nokleberg <a
 *         href="mailto:chris@nokleberg.com">chris@nokleberg.com</a>
 * @version $Id: JdkCompatibleProxy.java,v 1.3 2003/01/24 00:27:44 herbyderby
 *          Exp $
 */

public class CGLibProxyAdapter {

    private static Map<Class<?>, Object> generatedClasses = Collections.synchronizedMap(new WeakHashMap<Class<?>, Object>());

    private static final HashSet<String> PASSED_OBJECT_METHODS = new HashSet<String>();

    static {
        PASSED_OBJECT_METHODS.add("hashCode");
        PASSED_OBJECT_METHODS.add("equals");
        PASSED_OBJECT_METHODS.add("toString");
    }

    private static class HandlerAdapter implements MethodInterceptor {
        CGLibProxySerializer serializer = new CGLibProxySerializer();

        public HandlerAdapter(InvocationHandler handler, Class<?> superclass, Class<?>[] interfaces) {
            serializer.handler = handler;
            serializer.superclass = superclass.getName();
            if (interfaces != null) {
                serializer.interfaces = new String[interfaces.length];
                for (int i = 0; i < interfaces.length; i++) {
                    serializer.interfaces[i] = interfaces[i].getName();
                }
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see net.sf.cglib.proxy.MethodInterceptor#intercept(java.lang.Object,
         * java.lang.reflect.Method, java.lang.Object[],
         * net.sf.cglib.proxy.MethodProxy)
         */
        public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
            if (method.getName().equals("writeReplace")) {
                //                System.out.println("CGLIB EXECUTING WRITE REPLACE FOR: " + obj.getClass());
                return serializer;
            }

            if (method.getDeclaringClass() == Object.class && !PASSED_OBJECT_METHODS.contains(method.getName())) {
                return proxy.invokeSuper(obj, args);
            }
            return serializer.handler.invoke(obj, method, args);
        }
    }

    /**
     * CGLibProxySerializer Since the remote system to which we write the proxy
     * will have no idea what the proxy class is we replace the proxy class with
     * this guy, and when it is deserialized we create a new proxy instance.
     * 
     * @author cmacnaug
     * @version 1.0
     */
    private static class CGLibProxySerializer implements Serializable {
        private static final long serialVersionUID = 1L;
        private InvocationHandler handler;
        private String superclass;
        private String[] interfaces;

        @SuppressWarnings("serial")
        public Object readResolve() throws ObjectStreamException {
            try {
                ClassLoader cl = JMSRemoteSystem.INSTANCE.getUserClassLoader(this);
                Class<?> superclass = cl.loadClass(this.superclass);
                Class<?>[] interfaces = null;

                if (this.interfaces != null) {
                    interfaces = new Class<?>[this.interfaces.length];

                    for (int i = 0; i < this.interfaces.length; i++) {
                        interfaces[i] = cl.loadClass(this.interfaces[i]);
                    }
                }
                return CGLibProxyAdapter.newProxyInstance(superclass, interfaces, handler);
            } catch (final ClassNotFoundException e) {
                ObjectStreamException oe = new ObjectStreamException(e.getMessage()) {};
                oe.initCause(e);
                throw oe;
            } catch (ExportException ee) {
                ObjectStreamException oe = new ObjectStreamException(ee.getMessage()) {};
                oe.initCause(ee);
                throw oe;
            }
        }
    }

    public static InvocationHandler getInvocationHandler(Object proxy) {
        return ((HandlerAdapter) ((Factory) proxy).getCallback(0)).serializer.handler;
    }

    public static boolean isProxyClass(Class<?> cl) {
        //return generatedClasses.containsKey(cl);
        return CGILibSerializableProxy.class.isAssignableFrom(cl);
    }

    public static Object newProxyInstance(Class<?> superclass, Class<?>[] interfaces, InvocationHandler h) throws ExportException {
        HandlerAdapter ha = new HandlerAdapter(h, superclass, interfaces);

        if (interfaces != null) {
            Class<?>[] source = interfaces;
            interfaces = new Class<?>[source.length + 1];
            System.arraycopy(source, 0, interfaces, 1, source.length);
        } else {
            interfaces = new Class<?>[1];
        }

        //Pop in the CGILibSerializableProxy interface to support serialization of the proxy:
        interfaces[0] = CGILibSerializableProxy.class;

        //Check that we have a no arg constructor:
        try {
            superclass.getDeclaredConstructor(new Class<?> [] {});
        } catch (NoSuchMethodException nsme) {
            throw new ExportException("Can't export " + superclass.getName() + " because it doesn't define a no arg constructor. Try exporting with specified service interfaces", nsme);
        }

        //        System.out.println("CGLIB CREATING PROXY for " + superclass.getName() + " with interfaces: " + Arrays.asList(interfaces));
        Enhancer e = new Enhancer();
        e.setClassLoader(JMSRemoteSystem.INSTANCE.getUserClassLoader(ha));
        e.setSuperclass(superclass);
        e.setInterfaces(interfaces);
        e.setCallback(ha);
        Object obj = e.create();
        generatedClasses.put(obj.getClass(), null);
        return obj;
    }

    public static interface CGILibSerializableProxy extends Serializable {
        public Object writeReplace() throws ObjectStreamException;
    }

}
