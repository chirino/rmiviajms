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