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

package org.apache.iotdb.confignode.manager.pipe.plugin;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.plugin.PipePluginInformation;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.confignode.manager.pipe.PipeManager;
import org.apache.iotdb.confignode.persistence.pipe.PipePluginInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PipePluginCoordinator {

  private final PipeManager pipeManager;

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginCoordinator.class);

  private final PipePluginInfo pipePluginInfo;

  private final long planSizeLimit =
      ConfigNodeDescriptor.getInstance()
              .getConf()
              .getConfigNodeRatisConsensusLogAppenderBufferSize()
          - IoTDBConstant.RAFT_LOG_BASIC_SIZE;

  public PipePluginCoordinator(PipeManager pipeManager, PipePluginInfo pipePluginInfo) {
    this.pipeManager = pipeManager;
    this.pipePluginInfo = pipePluginInfo;
  }

  public TSStatus createPipePlugin(TCreatePipePluginReq req) {
    pipePluginInfo.acquirePipePluginTableLock();
    try {
      final String pluginName = req.getPluginName().toUpperCase(),
          jarMD5 = req.getJarMD5(),
          jarName = req.getJarName();
      final byte[] jarFile = req.getJarFile();
      pipePluginInfo.validate(pluginName, jarName, jarMD5);

      final PipePluginInformation pipePluginInformation =
          new PipePluginInformation(pluginName, req.getClassName(), jarName, jarMD5);
      final boolean needToSaveJar = pipePluginInfo.needToSaveJar(jarName);

      LOGGER.info(
          "Start to create PipePlugin [{}] on Data Nodes, needToSaveJar[{}]",
          pluginName,
          needToSaveJar);

      final TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(
              createPipePluginOnDataNodes(pipePluginInformation, needToSaveJar ? jarFile : null));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dataNodesStatus;
      }

      CreatePipePluginPlan createPluginPlan =
          new CreatePipePluginPlan(
              pipePluginInformation, needToSaveJar ? new Binary(jarFile) : null);
      if (needToSaveJar && createPluginPlan.getSerializedSize() > planSizeLimit) {
        return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    "Fail to create PipePlugin[%s], the size of Jar is too large, you should increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                    pluginName));
      }

      LOGGER.info("Start to add PipePlugin [{}] to PipePluginTable", pluginName);

      return pipeManager
          .getConfigManager()
          .getConsensusManager()
          .write(createPluginPlan)
          .getStatus();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      pipePluginInfo.releasePipePluginTableLock();
    }
  }

  private List<TSStatus> createPipePluginOnDataNodes(
      PipePluginInformation pipePluginInformation, byte[] jarFile) throws IOException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        pipeManager.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final TCreatePipePluginInstanceReq req =
        new TCreatePipePluginInstanceReq(
            pipePluginInformation.serialize(), ByteBuffer.wrap(jarFile));
    AsyncClientHandler<TCreatePipePluginInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CREATE_PIPE_PLUGIN, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus dropPipePlugin(String pluginName) {
    pluginName = pluginName.toUpperCase();
    pipePluginInfo.acquirePipePluginTableLock();
    try {
      pipePluginInfo.validate(pluginName);

      TSStatus result = RpcUtils.squashResponseStatusList(dropPipePluginOnDataNodes(pluginName));
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }

      return pipeManager
          .getConfigManager()
          .getConsensusManager()
          .write(new DropPipePluginPlan(pluginName))
          .getStatus();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      pipePluginInfo.releasePipePluginTableLock();
    }
  }

  private List<TSStatus> dropPipePluginOnDataNodes(String pluginName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        pipeManager.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();

    final TDropPipePluginInstanceReq req = new TDropPipePluginInstanceReq(pluginName, false);

    AsyncClientHandler<TDropPipePluginInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.DROP_PIPE_PLUGIN, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TGetPipePluginTableResp getPipePluginTable() {
    try {
      return ((PipePluginTableResp)
              pipeManager
                  .getConfigManager()
                  .getConsensusManager()
                  .read(new GetPipePluginTablePlan())
                  .getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get PipePluginTable", e);
      return new TGetPipePluginTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) {
    try {
      return ((JarResp)
              pipeManager
                  .getConfigManager()
                  .getConsensusManager()
                  .read(new GetPipePluginJarPlan(req.getJarNameList()))
                  .getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get PipePluginJar", e);
      return new TGetJarInListResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
