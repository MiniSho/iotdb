package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.commons.pipe.PipePluginTable;
import org.apache.iotdb.commons.pipe.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  private PipePluginInfo pipePluginInfo;
  private final ReentrantLock pipePluginTableLock = new ReentrantLock();
  private static final String snapshotFileName = "pipe_info.bin";

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo();
  }


  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return false;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
  }

  public void acquirePipePluginTableLock() {
    pipePluginTableLock.lock();
  }
}
