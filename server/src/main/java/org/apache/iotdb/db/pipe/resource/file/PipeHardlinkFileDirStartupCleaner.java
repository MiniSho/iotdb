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

package org.apache.iotdb.db.pipe.resource.file;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeHardlinkFileDirStartupCleaner {

  private static final AtomicBoolean isCleaned = new AtomicBoolean(false);

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHardlinkFileDirStartupCleaner.class);

  /**
   * Delete the data directory and all of its subdirectories that contain the
   * PipeConfig.PIPE_TSFILE_DIR_NAME directory.
   */
  public static void clean() {
    isCleaned.set(false);
    CompletableFuture.runAsync(PipeHardlinkFileDirStartupCleaner::doClean);
  }

  private static void doClean() {
    long totalStartTs = System.currentTimeMillis();
    for (String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      LOGGER.info("PipeHardlinkFileDirStartupCleaner.clean started, dataDir: {}", dataDir);
      try {
        for (File file :
            FileUtils.listFilesAndDirs(
                new File(dataDir), DirectoryFileFilter.INSTANCE, DirectoryFileFilter.INSTANCE)) {
          if (file.isDirectory()
              && file.getName().equals(PipeConfig.getInstance().getPipeHardlinkTsFileDirName())) {
            LOGGER.info(
                "pipe hardlink tsfile dir found, deleting it: {}, result: {}",
                file,
                FileUtils.deleteQuietly(file));
          }
        }
      } catch (Exception e) {
        LOGGER.warn("PipeHardlinkFileDirStartupCleaner.clean failed", e);
      }
    }
    LOGGER.info(
        "PipeHardlinkFileDirStartupCleaner finished, total cost: {}ms",
        System.currentTimeMillis() - totalStartTs);

    isCleaned.set(true);
  }

  public static boolean isHardlinkCleaned() {
    return isCleaned.get();
  }

  private PipeHardlinkFileDirStartupCleaner() {
    // util class
  }
}
