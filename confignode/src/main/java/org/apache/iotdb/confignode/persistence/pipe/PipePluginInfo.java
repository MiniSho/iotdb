package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.pipe.PipePluginInformation;
import org.apache.iotdb.commons.pipe.PipePluginTable;
import org.apache.iotdb.commons.pipe.service.PipePluginExecutableManager;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.pipe.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.pipePlugin.PipePluginTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final PipePluginTable pipePluginTable;

  private final Map<String, String> existedJarToMD5;

  private final PipePluginExecutableManager pipePluginExecutableManager;

  private final ReentrantLock pipePluginTableLock = new ReentrantLock();

  public PipePluginInfo() throws IOException {
    pipePluginTable = new PipePluginTable();
    existedJarToMD5 = new HashMap<>();
    pipePluginExecutableManager = PipePluginExecutableManager.setupAndGetInstance(CONFIG_NODE_CONF.getPipeTemporaryLibDir(), CONFIG_NODE_CONF.getPipeDir());
  }

  public void acquirePipePluginTableLock() {
    pipePluginTableLock.lock();
  }

  public void releasePipePluginTableLock() {
    pipePluginTableLock.unlock();
  }

  public void validate(String pluginName, String jarName, String jarMD5) {
    if (pipePluginTable.containsPipePlugin(pluginName)) {
      throw new PipeManagementException(String.format("Failed to create PipePlugin [%s], the same name PipePlugin has been created", pluginName));
    }

    if (existedJarToMD5.containsKey(jarName) && !existedJarToMD5.get(jarName).equals(jarMD5)) {
      throw new PipeManagementException(String.format("Failed to create PipePlugin [%s], the same name Jar [%s] but different MD5 [%s] has existed", pluginName, jarName, jarMD5));
    }
  }

  /**
   * Validate whether the PipePlugin can be dropped
   */
  public void validate(String pluginName) {
    if (pipePluginTable.containsPipePlugin(pluginName)) {
      return;
    }
    throw new PipeManagementException(String.format("Failed to drop PipePlugin [%s], the PipePlugin does not exist", pluginName));
  }

  public boolean needToSaveJar(String jarName) {
    return !existedJarToMD5.containsKey(jarName);
  }

  public TSStatus addPipePluginInTable(CreatePipePluginPlan physicalPlan) {
    try {
      final PipePluginInformation pipePluginInformation = physicalPlan.getPipePluginInformation();
      pipePluginTable.addPipePluginInformation(pipePluginInformation.getPluginName(), pipePluginInformation);
      existedJarToMD5.put(pipePluginInformation.getJarName(), pipePluginInformation.getJarMD5());
      if (physicalPlan.getJarFile() != null) {
        pipePluginExecutableManager.saveToInstallDir(ByteBuffer.wrap(physicalPlan.getJarFile().getValues()), pipePluginInformation.getJarName());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add PipePlugin [%s] in PipePlugin_Table on Config Nodes, because of %s",
              physicalPlan.getPipePluginInformation().getPluginName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public DataSet getPipePluginTable(){
    return new PipePluginTableResp(
        new TSStatus (TSStatusCode.SUCCESS_STATUS.getStatusCode()), Arrays.asList(pipePluginTable.getAllPipePluginInformation()) );
  }

  public JarResp getPipePluginJar(GetPipePluginJarPlan physicalPlan){
    List<ByteBuffer> jarList = new ArrayList<>();

    try{
      for( String jarName: physicalPlan.getJarNames()){
        jarList.add(ExecutableManager.transferToBytebuffer(PipePluginExecutableManager.getInstance().getFileStringUnderInstallByName(jarName)));
      }
    } catch (Exception e){
      LOGGER.error("Get PipePlugin_Jar failed", e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get PipePlugin_Jar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
    return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  public TSStatus dropPipePlugin(DropPipePluginPlan physicalPlan) {
    String pluginName = physicalPlan.getPluginName();
    if (pipePluginTable.containsPipePlugin(pluginName)){
      existedJarToMD5.remove(pipePluginTable.getPipePluginInformation(pluginName).getJarName());
      pipePluginTable.removePipePluginInformation(pluginName);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snape
  }
}
