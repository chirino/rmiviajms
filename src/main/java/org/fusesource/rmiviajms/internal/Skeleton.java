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

    Skeleton(JMSRemoteRef ref, Object target) {
        try {
            this.target = target;
            Class<?> clazz = this.target.getClass();
            if (CGLibProxyAdapter.isProxyClass(ref.getProxy().getClass())) {
                for (Method method : clazz.getMethods()) {
                    if(method.getDeclaringClass() == Object.class){
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
        try {
            Object result = invoke(request.methodSignature, request.args);
            return new Response(request.requestId, result, null);
        } catch (Throwable e) {
            return new Response(request.requestId, null, e);
        }
    }

    private Object invoke(String signature, Object[] args) throws Throwable {
        Method method = methods.get(signature);
        if (method == null) {
            throw new UnmarshalException("The remote object does contain the method: " + signature);
        }
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } catch (IllegalAccessException e) {
            throw new UnmarshalException("Could not invoke method: " + signature, e);
        } catch (Exception e) {
            throw new UnexpectedException(e.toString(), e);
        }
    }

}