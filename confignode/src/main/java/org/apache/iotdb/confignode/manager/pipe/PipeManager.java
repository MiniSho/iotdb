package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeManager.class);

  private final ConfigManager configManager;

  public PipeManager(ConfigManager configManager) {
    this.configManager = configManager;
  }
}
