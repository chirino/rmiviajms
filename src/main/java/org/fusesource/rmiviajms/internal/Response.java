package org.fusesource.rmiviajms.internal;

import java.io.Serializable;

/**
 * @author chirino
*/
final class Response implements Serializable {
    final long requestId;
    final Object result;
    final Throwable exception;
    boolean fromRemote;

    public Response(long requestId, Object result, Throwable exception) {
        this.exception = exception;
        this.requestId = requestId;
        this.result = result;
    }
}