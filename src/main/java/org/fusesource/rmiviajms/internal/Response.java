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