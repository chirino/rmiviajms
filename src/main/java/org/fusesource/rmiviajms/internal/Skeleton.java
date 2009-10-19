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

import java.rmi.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

/**
 * Every exported object gets one of these. It keeps a map that lets use convert
 * the messages into Method objects we can invoke.
 */
class Skeleton {
    final Object target;
    private final HashMap<String, Method> methods = new HashMap<String, Method>();
    private final JMSRemoteSystem remoteSystem;

    public ClassLoader getTargetClassLoader() {
        return remoteSystem.getUserClassLoader(target);
    }

    Skeleton(JMSRemoteSystem remoteSystem, JMSRemoteRef ref, Object target) {
        this.remoteSystem = remoteSystem;
        try {
            this.target = target;
            Class<?> clazz = this.target.getClass();
            if (CGLibProxyAdapter.isProxyClass(ref.getProxy().getClass())) {
                for (Method method : clazz.getMethods()) {
                    if (method.getDeclaringClass() == Object.class) {
                        continue;
                    }
                    //                    System.out.println("Class: " + clazz.getName() + " adding method: " + method.toGenericString());

                    String sig = JMSRemoteSystem.signature(method);
                    methods.put(sig, clazz.getMethod(method.getName(), method.getParameterTypes()));
                }
            } else {
                for (Class<?> intf : ref.getInterfaces()) {
                    for (Method method : intf.getMethods()) {
                        //                        System.out.println("Class: " + clazz.getName() + " adding method: " + method.toGenericString());

                        String sig = JMSRemoteSystem.signature(method);
                        methods.put(sig, intf.getMethod(method.getName(), method.getParameterTypes()));
                    }
                }
            }
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("target should implement all of the interfaces provided", e);
        }
    }

    public Response invoke(Request request) {
        //Invoke in the target's classloader:
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getTargetClassLoader());
        try {
            Object result = invoke(request.methodSignature, request.args);
            return new Response(request.requestId, result, null);
        } catch (Throwable e) {
            return new Response(request.requestId, null, e);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private Object invoke(String signature, Object[] args) throws Throwable {
        //Invoke in the target's classloader:
        Method method = methods.get(signature);
        if (method == null) {
            throw new UnmarshalException("The remote object does contain the method: " + signature);
        }
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getTargetClassLoader());
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } catch (IllegalAccessException e) {
            throw new UnmarshalException("Could not invoke method: " + signature, e);
        } catch (Exception e) {
            throw new UnexpectedException(e.toString(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

}