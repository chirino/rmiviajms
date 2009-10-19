![RMI via JMS][1]
=================

Description
-----------

[RMI via JMS][2] is a small java library that allows you to do Java Remote Method Invocations ([RMI][3])  via a Java Message Service ([JMS][4])  provider like [ActiveMQ][5].

Features
--------

* Supports the same development model that standard RMI uses.
* Supports a simpler development model what works with any kind of java object.
* Can issue asynchronous one way method invocations
* Objects can explicitly be bound to user defined JMS destinations.
* The JMS quality of service can be controlled via method level annotations.

Synopsis
--------

RMI is simple and powerful distributed development technology.  Unfortunately, the default Java implementation suffers from several shortcomings which are all associated with the fact method invocations over [JRMP][6] require the client's stubs/proxies to create TCP connections to the JVMs which have exported the objects.  You might think, "So what?".  But if you look at the properties of JRMP, you will notice that:

* Connectivity is very brittle in larger network scenarios due to:
  * RMI servers using of random TCP ports
  * Proxies contain the server address and run into problems when the server can be address multiple ways.
  * Therefore, applications exposing RMI services do not typically setup firewalls and are usually only accessed in local network scenarios.
* It is very hard to load balance client requests to multiple servers.
* If the server fails, or the network fails then the method call fails.
* It does not support broadcasting method invocations to multiple servers.

Lets compare that with the properties of using RMI over JMS:

* Proxies do NOT connect directly to the servers.  
* Proxies and servers create TCP connections to the JMS provider.
* The leading JMS providers run on well known ports, can federate on multiple hosts, and can optimize WAN traffic.
* Servers bind to JMS destinations which decouples a proxies from having to know on what host/port the server bound to.
* Multiple servers can bind to the same destination to achieve one of the following:
  * queue case: server load balancing and fail over
  * topic case: request broadcasting


Project Links
-------------

* [Project Home][2]
* [Release Downloads](http://rmiviajms.fusesource.org/downloads/index.html)
* [GitHub](http://github.com/chirino/rmiviajms/tree/master)
* Source: `git clone git://forge.fusesource.com/rmiviajms.git`

[1]: http://rmiviajms.fusesource.org/images/project-logo-small.png "RMI via JMS"
[2]: http://rmiviajms.fusesource.org "Project Home Page"
[3]: http://java.sun.com/docs/books/tutorial/rmi/overview.html
[4]: http://java.sun.com/products/jms/tutorial/1_3_1-fcs/doc/basics.html
[5]: http://fusesource.com/products/enterprise-activemq
[6]: http://en.wikipedia.org/wiki/Java_Remote_Method_Protocol
