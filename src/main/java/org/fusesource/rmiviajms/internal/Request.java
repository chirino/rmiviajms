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