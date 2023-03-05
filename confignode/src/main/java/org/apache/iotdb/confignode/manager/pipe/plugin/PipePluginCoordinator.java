package org.apache.iotdb.confignode.manager.pipe.plugin;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipePluginCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginCoordinator.class);

  public TSStatus createPipePlugin(TCreatePipePluginReq req) {

    try {
      final String pluginName = req.getPluginName().toUpperCase(), jarMD5 = req.getJarMD5(), jarName = req.getJarName();
      final byte[] jarFile = req.getJarFile();


    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    return null;
  }

  public TSStatus dropPipePlugin(String pluginName) {
    return null;
  }

  public TSStatus getPipePluginTable() {
    return null;
  }
}
