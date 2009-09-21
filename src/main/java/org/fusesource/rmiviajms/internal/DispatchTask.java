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

import java.rmi.NoSuchObjectException;

import javax.jms.ObjectMessage;
import javax.jms.JMSException;

/**
 * This task demarshalls an received message, invokes the exported object and
 * sends the response via the sender thread.
 */
final class DispatchTask implements Runnable {
    private final ObjectMessage msg;
    private final boolean oneway;
    private final JMSRemoteSystem remoteSystem;

    public DispatchTask(JMSRemoteSystem remoteSystem, ObjectMessage msg, boolean oneway) {
        this.remoteSystem = remoteSystem;
        this.msg = msg;
        this.oneway = oneway;
        //System.out.println("Created DispatchTask" + msg);
    }

    public void run() {
        try {
            //System.out.println("Executing DispatchTask" + msg);
            long oid = msg.getLongProperty(JMSRemoteSystem.MSG_PROP_OBJECT);
            long requestId = -1;
            if (!oneway) {
                requestId = msg.getLongProperty(JMSRemoteSystem.MSG_PROP_REQUEST);
            }

            Skeleton exportedObject = remoteSystem.exportedSkeletonsById.get(oid);
            Response response = null;
            Request request = null;
            if (exportedObject == null) {
                response = new Response(requestId, null, new NoSuchObjectException("" + oid));
            } else {
                try {
                    Thread.currentThread().setContextClassLoader(exportedObject.getTargetClassLoader());
                    request = (Request) (msg).getObject();
                    response = exportedObject.invoke(request);
                } catch (Throwable thrown) {
                    System.err.println("Error in rmi dispatch for " + exportedObject + "-" + exportedObject.getTargetClassLoader() + " / " + remoteSystem.getUserClassLoader());
                    thrown.printStackTrace();
                    
                    response = new Response(requestId, null, thrown);
                }
            }

            if (!oneway) {
                remoteSystem.sendResponse(msg, response);
            } else {
                if (response.exception != null) {
                    response.exception.printStackTrace();
                }
            }
        } catch (JMSException e) {
            // The request message must not have been properly created.. ignore for now.
            e.printStackTrace();
        } catch (Throwable thrown) {
            thrown.printStackTrace();
        }
    }
}