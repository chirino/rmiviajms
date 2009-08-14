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

import java.io.Serializable;
import java.util.Arrays;

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

    @Override
    public String toString() {
        return "Request{" +
                "objectId=" + objectId +
                ", methodSignature='" + methodSignature + '\'' +
                ", args=" + (args == null ? null : Arrays.asList(args)) +
                ", requestId=" + requestId +
                '}';
    }
}