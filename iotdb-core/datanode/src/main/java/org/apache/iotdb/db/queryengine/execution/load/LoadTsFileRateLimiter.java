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

import com.google.common.util.concurrent.RateLimiter;

public class LoadTsFileRateLimiter {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final RateLimiter loadWriteRateLimiter;

  private double throughoutMbPerSec = CONFIG.getCompactionWriteThroughputMbPerSec();

  private void setWritePointRate(final double throughoutMbPerSec) {
    double throughout = throughoutMbPerSec * 1024.0 * 1024.0;
    // if throughout = 0, disable rate limiting
    if (throughout <= 0) {
      throughout = Double.MAX_VALUE;
    }

    loadWriteRateLimiter.setRate(throughout);
  }

  public void acquireWrittenBytesWithLoadWriteRateLimiter(long writtenDataSizeInBytes) {
    if (throughoutMbPerSec != CONFIG.getLoadWriteThroughputMbPerSecond()) {
      throughoutMbPerSec = CONFIG.getLoadWriteThroughputMbPerSecond();
      setWritePointRate(throughoutMbPerSec);
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

  ///////////////////////////  Singleton  ///////////////////////////
  private LoadTsFileRateLimiter() {
    loadWriteRateLimiter =
        RateLimiter.create(
            IoTDBDescriptor.getInstance().getConfig().getCompactionWriteThroughputMbPerSec());
  }

  public static LoadTsFileRateLimiter getInstance() {
    return LoadTsFileRateLimiterHolder.INSTANCE;
  }

  private static class LoadTsFileRateLimiterHolder {
    private static final LoadTsFileRateLimiter INSTANCE = new LoadTsFileRateLimiter();
  }
}
