package org.fusesource.rmiviajms.internal;

import java.io.Serializable;

/**
 * @author chirino
*/
final class Request implements Serializable {
    final long objectId;
    final long requestId;
    final String methodSignature;
    final Object args[];

    public Request(long objectId, String methodSignature, Object[] args, long requestId) {
        this.args = args;
        this.methodSignature = methodSignature;
        this.objectId = objectId;
        this.requestId = requestId;
    }
}