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

import junit.framework.TestCase;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.Destination;

/**
 * @author chirino
 */
public class JMSRemoteObjectTest extends TestCase {

    private BrokerService broker;

    public static interface IHelloWorld extends Remote {
        public String hello() throws RemoteException;
        public void world(IHelloWorldCallback callback) throws RemoteException;

        @Oneway
        void slowOnewayOperations(int value) throws RemoteException, InterruptedException;
    }

    public  static interface IHelloWorldCallback extends Remote {
        public void execute(String value) throws RemoteException;
    }

    public static class HelloWorld implements IHelloWorld {
        AtomicLong value = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);

        public String hello() {
            return "hello";
        }

        public void world(IHelloWorldCallback callback) throws RemoteException {
            callback.execute("world");
        }

        public void slowOnewayOperations(int value) throws InterruptedException {
            Thread.sleep(1000);
            this.value.addAndGet(value);
            latch.countDown();
        }

    }

    public static class HelloWorldCallback extends JMSRemoteObject implements IHelloWorldCallback {
        String value;
        CountDownLatch latch = new CountDownLatch(1);

        HelloWorldCallback() throws RemoteException {
        }

        public HelloWorldCallback(Destination destination) throws RemoteException {
            super(destination);
        }

        public void execute(String value) {
            this.value=value;
            latch.countDown();
        }
    }

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:61616");
        broker.setUseJmx(false);
        broker.start();
    }

    @Override
    protected void tearDown() throws Exception {
        JMSRemoteObject.resetSystem();
        broker.stop();
    }

    public void testHelloWorld() throws RemoteException {
        HelloWorld object = new HelloWorld();
        Remote proxy = JMSRemoteObject.exportObject(object);
        assertTrue(proxy instanceof IHelloWorld);

        IHelloWorld hwp = (IHelloWorld) proxy;
        assertEquals("hello", hwp.hello());
    }


    public void testHelloWorldCallback() throws RemoteException, InterruptedException {
        HelloWorld object = new HelloWorld();
        IHelloWorld proxy = (IHelloWorld)JMSRemoteObject.exportObject(object);

        HelloWorldCallback callback = new HelloWorldCallback();
        proxy.world(callback);

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testHelloWorldAtKnownDestination() throws RemoteException {
        HelloWorld object = new HelloWorld();
        Remote proxy = JMSRemoteObject.exportObject(object, new ActiveMQQueue("FOO"));
        assertTrue(proxy instanceof IHelloWorld);

        IHelloWorld hwp = (IHelloWorld) proxy;
        assertEquals("hello", hwp.hello());
    }


    public void testHelloWorldCallbackAtKnownDestination() throws RemoteException, InterruptedException {
        HelloWorld object = new HelloWorld();
        IHelloWorld proxy = (IHelloWorld)JMSRemoteObject.exportObject(object);

        HelloWorldCallback callback = new HelloWorldCallback(new ActiveMQQueue("BAR"));
        proxy.world(callback);

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testOneway() throws RemoteException, InterruptedException {
        HelloWorld object = new HelloWorld();
        IHelloWorld proxy = (IHelloWorld)JMSRemoteObject.exportObject(object);

        // oneways will return before the remote invocation completes.
        proxy.slowOnewayOperations(1);
        assertEquals(0, object.value.get());

        // Now wait for it to complete...
        assertTrue(object.latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, object.value.get());
    }

    static public interface IBadOneWay extends Remote{
        @Oneway
        String badMethod() throws RemoteException;
    }
    static public class BadOneWay implements IBadOneWay {
        public String badMethod() throws RemoteException {
            return null;
        }
    }

    public void testInvalidOneway() throws RemoteException, InterruptedException {
        try {
            BadOneWay object = new BadOneWay();
            JMSRemoteObject.exportObject(object);
            fail("Expected RemoteException");
        } catch (RemoteException expected) {
        }
    }

    public void testProxyToKnownDestiantion() throws RemoteException, InterruptedException {
        ActiveMQQueue queue = new ActiveMQQueue("TEST");
        HelloWorld object = new HelloWorld();
        JMSRemoteObject.exportObject(object, queue);

        IHelloWorld proxy = JMSRemoteObject.toProxy(queue, IHelloWorld.class);
        assertEquals("hello", proxy.hello());
    }

}