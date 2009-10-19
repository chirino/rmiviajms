/**
 * Copyright (C) 2009 Progress Software, Inc. 
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
                    Thread.currentThread().setContextClassLoader(remoteSystem.getUserClassLoader(this));
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