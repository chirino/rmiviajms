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

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;

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

    public static interface IHelloWorldCallback extends Remote {
        public void execute(String value) throws RemoteException;
    }

    private static class HelloWorld implements IHelloWorld {
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

        public String toString() {
            return "Hello World String";
        }

    }

    private static class HelloWorldCallback extends JMSRemoteObject implements IHelloWorldCallback {
        String value;
        CountDownLatch latch = new CountDownLatch(1);

        HelloWorldCallback() throws RemoteException {
        }

        public HelloWorldCallback(String destination) throws RemoteException {
            super(destination);
        }

        public void execute(String value) {
            this.value = value;
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
        broker.waitUntilStopped();
    }

    public void testHelloWorld() throws Exception {
        HelloWorld object = new HelloWorld();
        Remote proxy = JMSRemoteObject.exportObject(object);
        assertTrue(proxy instanceof IHelloWorld);

        IHelloWorld hwp = (IHelloWorld) proxy;
        assertEquals("hello", hwp.hello());

        checkHelloWorldObjectMethods(object, proxy);
    }

    /**
     * Make sure that object methods are handled properly
     * 
     * @throws RemoteException
     */
    private static void checkHelloWorldObjectMethods(Object object, Object proxy) throws Exception {

        Thread.interrupted();

        assertFalse(proxy.equals(object));
        assertTrue(proxy.equals(proxy));
        assertNotSame(proxy.hashCode(), object.hashCode());
        assertNotSame(proxy.toString(), object.toString());

    }

    public void testHelloWorldCallback() throws RemoteException, InterruptedException {
        HelloWorld object = new HelloWorld();
        IHelloWorld proxy = (IHelloWorld) JMSRemoteObject.exportObject(object);

        HelloWorldCallback callback = new HelloWorldCallback();
        proxy.world(callback);

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testHelloWorldAtKnownDestination() throws RemoteException {
        HelloWorld object = new HelloWorld();
        Remote proxy = JMSRemoteObject.exportObject(object, "FOO");
        assertTrue(proxy instanceof IHelloWorld);

        IHelloWorld hwp = (IHelloWorld) proxy;
        assertEquals("hello", hwp.hello());
    }

    public void testHelloWorldCallbackAtKnownDestination() throws RemoteException, InterruptedException {
        HelloWorld object = new HelloWorld();
        IHelloWorld proxy = (IHelloWorld) JMSRemoteObject.exportObject(object);

        HelloWorldCallback callback = new HelloWorldCallback("BAR");
        proxy.world(callback);

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testOneway() throws RemoteException, InterruptedException {
        HelloWorld object = new HelloWorld();
        IHelloWorld proxy = (IHelloWorld) JMSRemoteObject.exportObject(object);

        // oneways will return before the remote invocation completes.
        proxy.slowOnewayOperations(1);
        assertEquals(0, object.value.get());

        // Now wait for it to complete...
        assertTrue(object.latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, object.value.get());
    }

    static public interface IBadOneWay extends Remote {
        @Oneway
        String badMethod() throws RemoteException;
    }

    static private class BadOneWay implements IBadOneWay {
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
        String queue = "TEST";
        HelloWorld object = new HelloWorld();
        JMSRemoteObject.exportObject(object, queue);

        IHelloWorld proxy = JMSRemoteObject.toProxy(queue, IHelloWorld.class);
        assertEquals("hello", proxy.hello());
    }

    public static interface IHelloWorldNotRemote {
        public String hello();

        public void world(IHelloWorldCallbackNotRemote callback);

        @Oneway
        void slowOnewayOperations(int value) throws InterruptedException;
    }

    public static interface IHelloWorldCallbackNotRemote {
        public void execute(String value);
    }

    private static class HelloWorldNotRemote implements IHelloWorldNotRemote {
        AtomicLong value = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);

        public String hello() {
            return "hello";
        }

        public void world(IHelloWorldCallbackNotRemote callback) {
            callback.execute("world");
        }

        public void slowOnewayOperations(int value) throws InterruptedException {
            Thread.sleep(1000);
            this.value.addAndGet(value);
            latch.countDown();
        }
    }

    private static class HelloWorldCallbackNotRemote implements IHelloWorldCallbackNotRemote {
        String value;
        CountDownLatch latch = new CountDownLatch(1);
        IHelloWorldCallbackNotRemote proxy;

        HelloWorldCallbackNotRemote() throws Exception {
            proxy = (IHelloWorldCallbackNotRemote) JMSRemoteObject.export(this, IHelloWorldCallbackNotRemote.class);
        }

        public HelloWorldCallbackNotRemote(String destination) throws Exception {
            proxy = (IHelloWorldCallbackNotRemote) JMSRemoteObject.export(this, destination, IHelloWorldCallbackNotRemote.class);
        }

        public IHelloWorldCallbackNotRemote getProxy() {
            return proxy;
        }

        public void execute(String value) {
            this.value = value;
            latch.countDown();
        }
    }

    public void testHelloWorldNotRemote() throws Exception {
        HelloWorldNotRemote object = new HelloWorldNotRemote();
        Remote proxy = JMSRemoteObject.export(object, new Class[] { IHelloWorldNotRemote.class });
        assertTrue(proxy instanceof IHelloWorldNotRemote);

        IHelloWorldNotRemote hwp = (IHelloWorldNotRemote) proxy;
        assertEquals("hello", hwp.hello());
        checkHelloWorldObjectMethods(object, proxy);
    }

    public void testHelloWorldCallbackNotRemote() throws Exception, InterruptedException {
        HelloWorldNotRemote object = new HelloWorldNotRemote();
        IHelloWorldNotRemote proxy = (IHelloWorldNotRemote) JMSRemoteObject.export(object, new Class[] { IHelloWorldNotRemote.class });

        HelloWorldCallbackNotRemote callback = new HelloWorldCallbackNotRemote();
        proxy.world(callback.getProxy());

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testHelloWorldAtKnownDestinationNotRemote() throws Exception {
        HelloWorldNotRemote object = new HelloWorldNotRemote();
        Remote proxy = JMSRemoteObject.export(object, "FOO", IHelloWorldNotRemote.class);

        assertTrue(proxy instanceof IHelloWorldNotRemote);

        IHelloWorldNotRemote hwp = (IHelloWorldNotRemote) proxy;
        assertEquals("hello", hwp.hello());
    }

    public void testHelloWorldCallbackAtKnownDestinationNotRemote() throws Exception, InterruptedException {
        HelloWorldNotRemote object = new HelloWorldNotRemote();
        IHelloWorldNotRemote proxy = (IHelloWorldNotRemote) JMSRemoteObject.export(object, new Class[] { IHelloWorldNotRemote.class });

        HelloWorldCallbackNotRemote callback = new HelloWorldCallbackNotRemote("BAR");
        proxy.world(callback.getProxy());

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public static class HelloWorldClass {
        AtomicLong value = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);

        public HelloWorldClass() {

        }

        public String hello() {
            return "hello";
        }

        public void world(HelloWorldCallbackClass callback) throws RemoteException {
            callback.execute("world");
        }

        @Oneway
        public void slowOnewayOperations(int value) throws InterruptedException {
            Thread.sleep(1000);
            this.value.addAndGet(value);
            latch.countDown();
        }
    }

    public static class HelloWorldCallbackClass {
        String value;
        CountDownLatch latch = new CountDownLatch(1);
        HelloWorldCallbackClass proxy;

        HelloWorldCallbackClass() throws Exception {
            proxy = (HelloWorldCallbackClass) JMSRemoteObject.export(this, (Class<?>[]) null);
        }

        public HelloWorldCallbackClass(String destination) throws Exception {
            proxy = (HelloWorldCallbackClass) JMSRemoteObject.export(this, destination, new Class[] {});
        }

        public HelloWorldCallbackClass getProxy() {
            return proxy;
        }

        public void execute(String value) {
            this.value = value;
            latch.countDown();
        }
    }

    public void testHelloWorldClass() throws Exception {
        HelloWorldClass object = new HelloWorldClass();
        Remote proxy = JMSRemoteObject.export(object, (Class<?>[]) null);
        assertTrue(proxy instanceof HelloWorldClass);

        HelloWorldClass hwp = (HelloWorldClass) proxy;
        assertEquals("hello", hwp.hello());

        checkHelloWorldObjectMethods(object, proxy);
    }

    public void testHelloWorldCallbackClass() throws Exception, InterruptedException {
        HelloWorldClass object = new HelloWorldClass();
        HelloWorldClass proxy = (HelloWorldClass) JMSRemoteObject.export(object, new Class[] {});

        HelloWorldCallbackClass callback = new HelloWorldCallbackClass();
        proxy.world(callback.getProxy());

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testHelloWorldAtKnownDestinationClass() throws Exception {
        HelloWorldClass object = new HelloWorldClass();
        Remote proxy = JMSRemoteObject.export(object, "FOO", (Class<?>[]) null);

        assertTrue(proxy instanceof HelloWorldClass);

        HelloWorldClass hwp = (HelloWorldClass) proxy;
        assertEquals("hello", hwp.hello());
    }

    public void testHelloWorldCallbackAtKnownDestinationClass() throws Exception, InterruptedException {
        HelloWorldClass object = new HelloWorldClass();
        HelloWorldClass proxy = (HelloWorldClass) JMSRemoteObject.export(object, (Class<?>[]) null);

        HelloWorldCallbackClass callback = new HelloWorldCallbackClass("BAR");
        proxy.world(callback.getProxy());

        assertTrue(callback.latch.await(5, TimeUnit.SECONDS));
        assertEquals("world", callback.value);

    }

    public void testOnewayClass() throws Exception, InterruptedException {
        HelloWorldClass object = new HelloWorldClass();
        HelloWorldClass proxy = (HelloWorldClass) JMSRemoteObject.export(object, (Class<?>[]) null);

        // oneways will return before the remote invocation completes.
        proxy.slowOnewayOperations(1);
        assertEquals(0, object.value.get());

        // Now wait for it to complete...
        assertTrue(object.latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, object.value.get());
    }

    public static class TopicTestClass {

    }

    public void testTopicMultibinding() throws Exception, InterruptedException {
        //System.out.println("Starting test");
        HelloWorldClass object1 = new HelloWorldClass();
        HelloWorldClass proxy1 = (HelloWorldClass) JMSRemoteObject.export(object1, JMSRemoteObject.MULTICAST_PREFIX + "TEST_TOPIC", (Class<?>[]) null);

        HelloWorldClass object2 = new HelloWorldClass();
        JMSRemoteObject.export(object2, JMSRemoteObject.MULTICAST_PREFIX + "TEST_TOPIC", (Class<?>[]) null);

        System.out.println("Testing slow one way");

        // One proxy invocation should invoke both objects.
        proxy1.slowOnewayOperations(1);

        assertEquals(0, object1.value.get());
        assertEquals(0, object2.value.get());

        // Now wait for it to complete...
        assertTrue(object1.latch.await(100, TimeUnit.SECONDS));
        assertTrue(object2.latch.await(100, TimeUnit.SECONDS));
        assertEquals(1, object1.value.get());
        assertEquals(1, object2.value.get());
    }

    public static class TestNoNoArgConstructorObject {
        TestNoNoArgConstructorObject(String foo) {

        }
    }

    public void testInvalidNoArgConstructorExport() throws Exception {
        TestNoNoArgConstructorObject shouldFail = new TestNoNoArgConstructorObject("fail");
        try {
            JMSRemoteObject.export(shouldFail);
        } catch (ExportException ee) {
            System.out.println("Got expected export exception: " + ee.getMessage());
            return;
        }
        
        fail("Didn't get expected export failure exception"); 

    }

}