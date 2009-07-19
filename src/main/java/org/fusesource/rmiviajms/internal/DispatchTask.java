package org.fusesource.rmiviajms.internal;

import javax.jms.ObjectMessage;
import javax.jms.JMSException;

/**
     * This task demarshalls an received message, invokes the exported object
 * and sends the response via the sender thread.
 */
final class DispatchTask implements Runnable {
    private final ObjectMessage msg;
    private JMSRemoteSystem remoteSystem;

    public DispatchTask(JMSRemoteSystem remoteSystem, ObjectMessage msg) {
        this.remoteSystem = remoteSystem;
        this.msg = msg;
    }

    public void run() {
        try {
            long oid = msg.getLongProperty("object");
            Skeleton exportedObject = remoteSystem.exportedSkeletonsById.get(oid);
            Thread.currentThread().setContextClassLoader(exportedObject.target.getClass().getClassLoader());
            Request request = (Request)(msg).getObject();
            Response response = exportedObject.invoke(request);
            remoteSystem.sendResponse(request, response);
        } catch (JMSException e) {
            // The request message must not have been properly created.. ignore for now.
        }
    }
}