/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport;

import java.util.concurrent.Semaphore;

/**
 * A useful base class for a transport implementation which has a background
 * reading thread.
 * 
 * 
 */
public abstract class TransportThreadSupport extends TransportSupport implements Runnable {

    private boolean daemon;
    private Thread[] runners = new Thread[1];
    protected Semaphore available = new Semaphore(1, true);
    // should be a multiple of 128k
    private long stackSize;

    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    protected void doStart() throws Exception {
        for (int i=0;i<runners.length;i++) {
            runners[i] = new Thread(null, this, "ActiveMQ Transport["+i+"]: " + toString(), stackSize);
            runners[i].setDaemon(daemon);
            runners[i].start();
        }
    }

    /**
     * @return the stackSize
     */
    public long getStackSize() {
        return this.stackSize;
    }

    /**
     * @param stackSize the stackSize to set
     */
    public void setStackSize(long stackSize) {
        this.stackSize = stackSize;
    }


  protected static final ThreadLocal<Boolean> released =
             new ThreadLocal<Boolean>() {
                 @Override protected Boolean initialValue() {
                     return Boolean.FALSE;
             }
         };
}
