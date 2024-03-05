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

package org.apache.iotdb.confignode.procedure.impl.pipe.mq.topic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.mq.meta.PipeMQTopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.CreatePipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.mq.topic.DropPipeMQTopicPlan;
import org.apache.iotdb.confignode.manager.pipe.mq.coordinator.topic.PipeMQTopicCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.mq.AbstractOperatePipeMQProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreatePipeMQTopicProcedure extends AbstractOperatePipeMQProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipeMQTopicProcedure.class);

  private TCreateTopicReq createTopicReq;
  private PipeMQTopicMeta pipeMQTopicMeta;

  public CreatePipeMQTopicProcedure(TCreateTopicReq createTopicReq) throws PipeException {
    super();
    this.createTopicReq = createTopicReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_TOPIC;
  }

  @Override
  protected void executeFromLock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreatePipeMQTopicProcedure: executeFromLock, try to acquire pipeMQ lock");

    final PipeMQTopicCoordinator pipeMQTopicCoordinator =
        env.getConfigManager().getMQManager().getPipeMQTopicCoordinator();

    pipeMQTopicCoordinator.lock();

    // check if the topic exists

    try {
      pipeMQTopicCoordinator.getPipeMQInfo().validateBeforeCreatingTopic(createTopicReq);
    } catch (PipeException e) {
      // The topic has already created, we should end the procedure
      LOGGER.warn(
          "Topic {} is already created, end the CreatePipeMQTopicProcedure({})",
          createTopicReq.getTopicName(),
          createTopicReq.getTopicName());
      setFailure(new ProcedureException(e.getMessage()));
      pipeMQTopicCoordinator.unlock();
      throw e;
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeMQTopicProcedure: executeFromOperateOnConfigNodes({})",
        pipeMQTopicMeta.getTopicName());

    final CreatePipeMQTopicPlan createPipeMQTopicPlan = new CreatePipeMQTopicPlan(pipeMQTopicMeta);

    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(createPipeMQTopicPlan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeMQTopicProcedure: executeFromOperateOnDataNodes({})",
        pipeMQTopicMeta.getTopicName());

    if (RpcUtils.squashResponseStatusList(env.pushSinglePipeMQTopicOnDataNode(pipeMQTopicMeta))
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return;
    }

    throw new PipeException(
        String.format(
            "Failed to create topic instance [%s] on data nodes", pipeMQTopicMeta.getTopicName()));
  }

  @Override
  protected void executeFromUnlock(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeMQTopicProcedure: executeFromUnlock({})", pipeMQTopicMeta.getTopicName());
    env.getConfigManager().getMQManager().getPipeMQTopicCoordinator().unlock();
  }

  @Override
  protected void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipeMQTopicProcedure: rollbackFromLock({})", pipeMQTopicMeta.getTopicName());
    env.getConfigManager().getMQManager().getPipeMQTopicCoordinator().unlock();
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeMQTopicProcedure: rollbackFromCreateOnConfigNodes({})",
        pipeMQTopicMeta.getTopicName());

    try {
      env.getConfigManager()
          .getConsensusManager()
          .write(new DropPipeMQTopicPlan(pipeMQTopicMeta.getTopicName()));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeMQTopicProcedure: rollbackFromCreateOnDataNodes({})",
        pipeMQTopicMeta.getTopicName());

    if (RpcUtils.squashResponseStatusList(
                env.dropSinglePipeMQTopicOnDataNode(pipeMQTopicMeta.getTopicName()))
            .getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(
          String.format(
              "Failed to rollback pipe mq topic [%s] on data nodes",
              pipeMQTopicMeta.getTopicName()));
    }
  }
}
