/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;

public class LoadTsFileRateLimiter {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private RateLimiter loadWriteRateLimiter;

  private AtomicDouble throughputMbPerSec =
      new AtomicDouble(CONFIG.getLoadWriteThroughputMbPerSecond());

  public LoadTsFileRateLimiter() {
    loadWriteRateLimiter = RateLimiter.create(throughputMbPerSec.get());
  }

  private void setWritePointRate(final double throughputMbPerSec) {
    double throughput = throughputMbPerSec * 1024.0 * 1024.0;
    // if throughput = 0, disable rate limiting
    if (throughput <= 0) {
      throughput = Double.MAX_VALUE;
    }

    loadWriteRateLimiter.setRate(throughput);
  }

  public void acquireWrittenBytesWithLoadWriteRateLimiter(long writtenDataSizeInBytes) {
    if (throughputMbPerSec.get() != CONFIG.getLoadWriteThroughputMbPerSecond()) {
      throughputMbPerSec.set(CONFIG.getLoadWriteThroughputMbPerSecond());
      setWritePointRate(throughputMbPerSec.get());
    }

    while (writtenDataSizeInBytes > 0) {
      if (writtenDataSizeInBytes > Integer.MAX_VALUE) {
        loadWriteRateLimiter.acquire(Integer.MAX_VALUE);
        writtenDataSizeInBytes -= Integer.MAX_VALUE;
      } else {
        loadWriteRateLimiter.acquire((int) writtenDataSizeInBytes);
        return;
      }
    }
  }

  //////////////////////////// Singleton ///////////////////////////////////////
  private static class LoadTsFileRateLimiterHolder {
    private static final LoadTsFileRateLimiter INSTANCE = new LoadTsFileRateLimiter();
  }

  public static LoadTsFileRateLimiter getInstance() {
    return LoadTsFileRateLimiterHolder.INSTANCE;
  }
}
