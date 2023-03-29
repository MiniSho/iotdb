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

package org.apache.iotdb.db.pipe.execution.executor;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@NotThreadSafe
public abstract class PipeSubtaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtaskExecutor.class);

  private final WrappedThreadPoolExecutor wrappedThreadPoolExecutor;
  private final ListeningExecutorService listeningExecutorService;

  private final Map<String, PipeSubtask> registeredIdSubtaskMapper;

  private int corePoolSize;

  protected PipeSubtaskExecutor(int corePoolSize, ThreadName threadName) {
    wrappedThreadPoolExecutor =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(corePoolSize, threadName.getName());
    listeningExecutorService = MoreExecutors.listeningDecorator(wrappedThreadPoolExecutor);

    registeredIdSubtaskMapper = new ConcurrentHashMap<>();

    this.corePoolSize = corePoolSize;
  }

  /////////////////////// subtask management ///////////////////////

  public final void register(PipeSubtask subtask) {
    if (registeredIdSubtaskMapper.containsKey(subtask.getTaskID())) {
      LOGGER.warn("The subtask {} is already registered.", subtask.getTaskID());
      return;
    }

    registeredIdSubtaskMapper.put(subtask.getTaskID(), subtask);
    subtask.bindExecutorService(listeningExecutorService);
  }

  public final void start(String subTaskID) {
    if (!registeredIdSubtaskMapper.containsKey(subTaskID)) {
      LOGGER.warn("The subtask {} is not registered.", subTaskID);
      return;
    }

    final PipeSubtask subtask = registeredIdSubtaskMapper.get(subTaskID);
    if (subtask.isSubmittingSelf()) {
      LOGGER.info("The subtask {} is already running.", subTaskID);
    } else {
      subtask.allowSubmittingSelf();
      subtask.submitSelf();
      LOGGER.info("The subtask {} is started to submit self.", subTaskID);
    }
  }

  public final void stop(String subTaskID) {
    if (!registeredIdSubtaskMapper.containsKey(subTaskID)) {
      LOGGER.warn("The subtask {} is not registered.", subTaskID);
      return;
    }

    registeredIdSubtaskMapper.get(subTaskID).disallowSubmittingSelf();
  }

  public final void deregister(String subTaskID) {
    stop(subTaskID);

    registeredIdSubtaskMapper.remove(subTaskID);
  }

  /////////////////////// executor management  ///////////////////////

  public final void shutdown() {
    if (listeningExecutorService != null) {
      listeningExecutorService.shutdown();
    }
  }

  public final void adjustExecutorThreadNumber(int threadNum) {
    corePoolSize = threadNum;
    wrappedThreadPoolExecutor.setCorePoolSize(threadNum);
  }

  public final int getExecutorThreadNumber() {
    return corePoolSize;
  }

  /////////////////////// test only ///////////////////////

  @TestOnly
  public ListeningExecutorService getListeningExecutorService() {
    return listeningExecutorService;
  }
}
